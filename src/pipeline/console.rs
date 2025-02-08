use chrono::Duration as ChronoDuration;
use chrono::{DateTime, Utc};
use gasket::{
    framework::WorkerError,
    metrics::Reading,
    runtime::{StagePhase, TetherState},
};
use lazy_static::lazy_static;
use log::Log;
use ratatui::prelude::Layout;
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Padding};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::Mutex;

use crate::crosscut;

use super::{Context, StageTypes};

use crossterm::{
    event::{self, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{prelude::*, widgets::Paragraph};
use std::io::{stdout, Stdout};

fn friendly_duration(seconds: i64) -> String {
    let duration = ChronoDuration::seconds(seconds);
    let days = duration.num_days();
    let hours = duration.num_hours() % 24;
    let minutes = duration.num_minutes() % 60;

    if days > 0 {
        format!("{} days and {} hours", days, hours)
    } else if hours > 0 {
        format!("{} hours and {} minutes", hours, minutes)
    } else if minutes > 0 {
        format!("{} minutes", minutes)
    } else {
        format!("{} seconds", seconds)
    }
}

fn remaining_time(rate_per_second: f64, total_items: i64, processed_items: i64) -> String {
    if rate_per_second == 0.0 {
        return "∞".to_string();
    }

    let remaining_items = (total_items - processed_items) as f64;
    let remaining_seconds = (remaining_items / rate_per_second).ceil() as i64;

    friendly_duration(remaining_seconds)
}

#[derive(clap::ValueEnum, Clone)]
pub enum Mode {
    /// shows progress as a plain sequence of logs
    Plain,
    /// shows aggregated progress and metrics
    TUI,
}

impl Default for Mode {
    fn default() -> Self {
        Mode::Plain
    }
}

#[derive(Clone, Debug)]
struct MeteredNumber {
    value: u64,
}

impl Default for MeteredNumber {
    fn default() -> Self {
        Self { value: 0 }
    }
}

impl MeteredNumber {
    pub fn set(&mut self, to: u64) {
        self.value = to;
    }
}

#[derive(Clone, Debug)]
struct MeteredString {
    value: String,
}

impl Default for MeteredString {
    fn default() -> Self {
        Self { value: "".into() }
    }
}

impl MeteredString {
    pub fn set(&mut self, to: &str) {
        self.value = to.to_string();
    }
}

impl From<&str> for MeteredString {
    fn from(item: &str) -> Self {
        MeteredString {
            value: item.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
enum MeteredValue {
    Numerical(MeteredNumber),
    Label(MeteredString),
}

impl MeteredValue {
    pub fn set_num(&mut self, to: u64) {
        match self {
            MeteredValue::Numerical(metered_number) => {
                metered_number.set(to);
            }
            MeteredValue::Label(_) => {}
        }
    }

    pub fn set_str(&mut self, to: &str) {
        match self {
            MeteredValue::Numerical(_) => {}
            MeteredValue::Label(metered_string) => {
                metered_string.set(to);
            }
        }
    }

    pub fn get_num(&self) -> u64 {
        match self {
            MeteredValue::Numerical(metered_number) => metered_number.value.clone(),
            MeteredValue::Label(_) => 0,
        }
    }

    pub fn get_string(&self) -> String {
        match self {
            MeteredValue::Numerical(_) => "".into(),
            MeteredValue::Label(metered_string) => metered_string.value.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    timestamp: Duration,
    chain_bar_depth: MeteredValue,
    chain_bar_progress: MeteredValue,
    blocks_processed: MeteredValue,
    transactions: MeteredValue,
    chain_era: MeteredValue,
    sources_status: MeteredValue,
    enrich_status: MeteredValue,
    reducer_status: MeteredValue,
    storage_status: MeteredValue,
    enrich_hit: MeteredValue,
    enrich_miss: MeteredValue,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            timestamp: Default::default(),
            chain_era: MeteredValue::Label(Default::default()),
            chain_bar_depth: MeteredValue::Numerical(Default::default()),
            chain_bar_progress: MeteredValue::Numerical(Default::default()),
            blocks_processed: MeteredValue::Numerical(Default::default()),
            transactions: MeteredValue::Numerical(Default::default()),
            sources_status: MeteredValue::Label(Default::default()),
            enrich_status: MeteredValue::Label(Default::default()),
            reducer_status: MeteredValue::Label(Default::default()),
            storage_status: MeteredValue::Label(Default::default()),
            enrich_hit: MeteredValue::Numerical(Default::default()),
            enrich_miss: MeteredValue::Numerical(Default::default()),
        }
    }
}

impl MetricsSnapshot {
    fn get_metrics_key(&self, prop_name: &str) -> Option<MeteredValue> {
        match prop_name {
            "chain_era" => Some(self.chain_era.clone()),
            "chain_bar_depth" => Some(self.chain_bar_depth.clone()),
            "chain_bar_progress" => Some(self.chain_bar_progress.clone()),
            "blocks_processed" => Some(self.blocks_processed.clone()),
            "transactions" => Some(self.transactions.clone()),
            _ => None,
        }
    }
}

struct LogBuffer {
    vec: Arc<Mutex<Vec<(String, String)>>>,
    capacity: usize,
}

pub struct BlockGraph {
    vec: Vec<MetricsSnapshot>,
    base_time: Instant,
    capacity: usize,
    last_dropped: Option<MetricsSnapshot>,
}

impl LogBuffer {
    // pub fn new(capacity: usize) -> Self {
    //     //let base_time = Instant::now();

    //     Self {
    //         vec: Arc::new(Mutex::new(vec![])),
    //         capacity,
    //     }
    // }

    // pub async fn push(&mut self, ele: (String, String)) {
    //     let mut v = self.vec.lock().await;
    //     if v.len() == self.capacity {
    //         v.remove(0);
    //     }

    //     v.push(ele.clone());
    // }

    pub async fn all(&self) -> Vec<(String, String)> {
        self.vec.lock().await.clone()
    }
}

impl BlockGraph {
    pub fn new(capacity: usize) -> Self {
        let base_time = Instant::now();
        let vec: Vec<MetricsSnapshot> = Vec::default();

        Self {
            vec,
            base_time,
            capacity,
            last_dropped: None,
        }
    }

    pub fn push(&mut self, ele: MetricsSnapshot) {
        if self.vec.len() == self.capacity {
            self.last_dropped = Some(self.vec[0].clone());
            self.vec.remove(0);
        }

        self.vec.push(ele);
    }

    pub fn get(&self, index: usize) -> &MetricsSnapshot {
        self.vec.get(index).unwrap()
    }

    pub fn timestamp_window(&self) -> (f64, f64) {
        let mut min: Duration = Default::default();
        let mut max: Duration = Default::default();

        for snapshot in self.vec.clone() {
            let current = snapshot.timestamp;

            if current < min || min == Duration::default() {
                min = current;
            }

            if current > max {
                max = current;
            }
        }

        (min.as_secs_f64(), max.as_secs_f64())
    }

    fn get_prop_value_for_index(&self, prop_name: &str, vec_idx: usize) -> Option<MeteredValue> {
        match self.vec.get(vec_idx) {
            Some(snapshot) => match snapshot.get_metrics_key(prop_name) {
                Some(metrics_value) => Some(metrics_value),
                None => None,
            },
            None => None,
        }
    }

    pub fn rates_for_snapshot_prop(&self, prop_name: &str) -> [(f64, f64); RING_DEPTH] {
        let mut rates: Vec<(f64, f64)> = Default::default();

        let mut stub_metrics_snapshot: MetricsSnapshot = Default::default();
        stub_metrics_snapshot.timestamp = match self.vec.clone().get(0) {
            Some(s) => s
                .timestamp
                .clone()
                .checked_sub(Duration::from_millis(1000))
                .unwrap_or(Duration::default()),
            _ => stub_metrics_snapshot.timestamp,
        };

        let last_dropped = match self.last_dropped.clone() {
            Some(previous_snapshot) => previous_snapshot,
            None => stub_metrics_snapshot,
        };

        for (i, current_snapshot) in self.vec.clone().into_iter().enumerate() {
            let previous_snapshot = if i > 0 {
                self.vec.get(i - 1).unwrap().clone()
            } else {
                last_dropped.clone()
            };

            let previous_duration = previous_snapshot.timestamp;
            let current_duration = current_snapshot.timestamp;

            let previous_value = if i > 0 {
                self.get_prop_value_for_index(prop_name, i - 1)
                    .unwrap_or(MeteredValue::Numerical(MeteredNumber { value: 0 }))
                    .get_num()
            } else {
                previous_snapshot
                    .get_metrics_key(prop_name)
                    .unwrap()
                    .get_num()
            };

            let current_value = self
                .get_prop_value_for_index(prop_name, i)
                .unwrap()
                .get_num();

            let time_diff = if current_duration > previous_duration {
                (current_duration - previous_duration).as_secs_f64()
            } else {
                0.0
            };

            let value_diff = current_value - previous_value;

            let rate_of_increase = if time_diff > 0.0 && value_diff > 0 {
                value_diff as f64 / time_diff
            } else {
                0.0
            };

            rates.push((current_snapshot.timestamp.as_secs_f64(), rate_of_increase));
        }

        let mut final_rates: [(f64, f64); RING_DEPTH] = [(0.0, 0.0); RING_DEPTH];

        for (i, _) in final_rates.clone().iter().enumerate() {
            if i + 1 <= rates.len() {
                final_rates[i] = rates[i];
            }
        }

        final_rates
    }

    pub fn window_for_snapshot_prop(&self, prop_name: &str) -> (f64, f64) {
        let mut min: f64 = 0.0;
        let mut max: f64 = 0.0;

        let prop_rates = self.rates_for_snapshot_prop(prop_name);

        for snapshot in prop_rates {
            if min == 0.0 {
                min = snapshot.1;
            }

            if (snapshot.1) < min {
                min = snapshot.1;
            }

            if (snapshot.1) > max {
                max = snapshot.1;
            }
        }

        (min, max)
    }
}

struct TuiConsole {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    metrics_buffer: BlockGraph,
}

impl Deref for TuiConsole {
    type Target = Terminal<CrosstermBackend<std::io::Stdout>>;

    fn deref(&self) -> &Self::Target {
        &self.terminal
    }
}

impl DerefMut for TuiConsole {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.terminal
    }
}

pub fn i64_to_string(mut i: i64) -> String {
    let mut bytes = Vec::new();

    while i != 0 {
        let byte = (i & 0xFF) as u8;
        // Skip if it's a padding byte
        if byte != 0 {
            bytes.push(byte);
        }
        i >>= 8;
    }

    let s = std::string::String::from_utf8(bytes).unwrap();

    s.chars().rev().collect::<String>()
}

const RING_DEPTH: usize = 100;

impl TuiConsole {
    fn new() -> Self {
        stdout().execute(EnterAlternateScreen).unwrap();
        enable_raw_mode().unwrap();
        Self {
            terminal: Terminal::new(CrosstermBackend::new(stdout())).unwrap(),
            metrics_buffer: BlockGraph::new(RING_DEPTH),
        }
    }

    async fn draw(&mut self, ctx: Option<Arc<Context>>, snapshot: &MetricsSnapshot) {
        //let current_era = snapshot.chain_era.get_string();

        let log_buffer_partial = LOG_BUFFER.all().await;
        let log_buffer_partial = log_buffer_partial.iter().rev();

        let mut date_string = None;
        let mut has_ctx = false;

        match ctx {
            Some(ctx) => {
                let provider = crosscut::time::NaiveProvider::new(ctx.clone()).await;
                let wallclock = provider.slot_to_wallclock(snapshot.chain_bar_progress.get_num());
                let d = SystemTime::UNIX_EPOCH + Duration::from_secs(wallclock);
                let datetime = DateTime::<Utc>::from(d);
                date_string = Some(datetime.format("%Y-%m-%d %H:%M:%S").to_string());
                has_ctx = true;
            }

            None => {}
        }

        self.terminal
            .draw(|frame| {
                let layout = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints(vec![
                        Constraint::Length(10),
                        Constraint::Min(1),
                        Constraint::Length(1),
                        Constraint::Length(1),
                        Constraint::Length(5),
                        Constraint::Length(1),
                        Constraint::Length(1),
                        Constraint::Length(2),
                    ])
                    .split(frame.size());

                let mut log_buffer_string = String::default();
                for entry in log_buffer_partial.rev().take(layout[1].height as usize - 2) {
                    log_buffer_string += &format!("{} {}\n", entry.0, entry.1);
                }

                let top_status_layout = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints(vec![Constraint::Length(26), Constraint::Min(60)])
                    .split(layout[0]);

                let progress_layout = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints(vec![
                        Constraint::Length(15),
                        Constraint::Min(50),
                        Constraint::Length(15),
                    ])
                    .split(layout[6]);

                let mixed_chart_layout = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints(vec![
                        Constraint::Max(7),
                        Constraint::Min(10),
                        Constraint::Max(7),
                    ])
                    .split(layout[4]);

                let chart_blocks_axis = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints(vec![
                        Constraint::Percentage(48),
                        Constraint::Min(1),
                        Constraint::Max(1),
                    ])
                    .split(mixed_chart_layout[0]);

                let chart_tx_axis = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints(vec![
                        Constraint::Percentage(48),
                        Constraint::Min(1),
                        Constraint::Max(1),
                    ])
                    .split(mixed_chart_layout[2]);

                let progress = ratatui::widgets::Gauge::default()
                    .block(
                        ratatui::widgets::Block::default()
                            .borders(Borders::NONE)
                            .style(Style::default().bg(Color::DarkGray))
                            .padding(Padding::new(0, 0, 0, 0)),
                    )
                    .gauge_style(Style::new().blue())
                    .percent(
                        match snapshot.chain_bar_depth.get_num() > 0
                            && snapshot.chain_bar_progress.get_num() > 0
                        {
                            true => (snapshot.chain_bar_progress.get_num() as f64
                                / snapshot.chain_bar_depth.get_num() as f64
                                * 100.0)
                                .round() as u16,
                            false => 0,
                        },
                    );

                //let bottom_pane = ratatui::widgets::Block::default().title("Hi");

                //frame.render_widget(bottom_pane, layout[0]);

                frame.render_widget(
                    Paragraph::new(log_buffer_string)
                        .block(
                            ratatui::widgets::Block::new()
                                .padding(Padding::new(
                                    3, // left
                                    1, // right
                                    1, // top
                                    1, // bottom
                                ))
                                .fg(Color::Blue),
                        )
                        .alignment(Alignment::Left),
                    layout[1],
                );

                frame.render_widget(
                    Paragraph::new(snapshot.chain_era.get_string())
                        .block(ratatui::widgets::Block::new().padding(Padding::new(
                            0, // left
                            1, // right
                            0, // top
                            0, // bottom
                        )))
                        .alignment(Alignment::Right),
                    progress_layout[0],
                );

                let progress_footer_layout = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints(vec![
                        Constraint::Length(progress_layout[2].width - 1),
                        Constraint::Max(20),
                        Constraint::Min(10),
                        Constraint::Length(progress_layout[2].width - 1),
                    ])
                    .split(layout[7]);

                frame.render_widget(
                    Paragraph::new(include_str!("./../../assets/boot.txt"))
                        .block(ratatui::widgets::Block::new().padding(Padding::new(
                            3, // left
                            0, // right
                            2, // top
                            0, // bottom
                        )))
                        .alignment(Alignment::Left),
                    top_status_layout[0],
                );

                // frame.render_widget(
                //     Paragraph::new("hi")
                //         .block(ratatui::widgets::Block::new())
                //         .alignment(Alignment::Left),
                //     layout[1],
                // );

                match has_ctx {
                    true => {
                        let status_sublayout = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints(vec![
                                Constraint::Length(2),
                                Constraint::Length(1),
                                Constraint::Min(1),
                            ])
                            .split(top_status_layout[1]);

                        let sources_status = snapshot.sources_status.get_string();
                        let enrich_status = snapshot.enrich_status.get_string();
                        let reducer_status = snapshot.reducer_status.get_string();
                        let storage_status = snapshot.storage_status.get_string();

                        frame.render_widget(
                            Paragraph::new("Gasket Workers")
                                .block(ratatui::widgets::Block::new().padding(Padding::new(
                                    0, // left
                                    0, // right
                                    0, // top
                                    0, // bottom
                                )))
                                .bold()
                                .alignment(Alignment::Left),
                            status_sublayout[1],
                        );

                        frame.render_widget(
                            Paragraph::new(format!(
                                "{} Source\n{} Enrich ({} hits / {} misses)\n{} Reduce\n{} Storage",
                                if sources_status.is_empty() {
                                    "⧗".to_string()
                                } else {
                                    sources_status
                                },
                                if enrich_status.is_empty() {
                                    "⚠".to_string()
                                } else {
                                    enrich_status
                                },
                                snapshot.enrich_hit.get_num(),
                                snapshot.enrich_miss.get_num(),
                                if reducer_status.is_empty() {
                                    "⧗".to_string()
                                } else {
                                    reducer_status
                                },
                                if storage_status.is_empty() {
                                    "⧗".to_string()
                                } else {
                                    storage_status
                                }
                            ))
                            .block(ratatui::widgets::Block::new().padding(Padding::new(
                                0, // left
                                0, // right
                                0, // top
                                0, // bottom
                            )))
                            .alignment(Alignment::Left),
                            status_sublayout[2],
                        );
                    }
                    false => {
                        frame.render_widget(
                            Paragraph::new("Opening historic block buffer...")
                                .block(ratatui::widgets::Block::new().padding(Padding::new(
                                    0, // left
                                    0, // right
                                    2, // top
                                    0, // bottom
                                )))
                                .alignment(Alignment::Left),
                            top_status_layout[1],
                        );
                    }
                }

                // frame.render_widget(
                //     Paragraph::new(snapshot.chain_bar_progress.get_str())
                //         .block(Block::new().padding(Padding::new(
                //             0, // left
                //             0, // right
                //             0, // top
                //             0, // bottom
                //         )))
                //         .alignment(Alignment::Left),
                //     progress_footer_layout[0],
                // );

                frame.render_widget(progress, progress_layout[1]);

                frame.render_widget(
                    Paragraph::new(snapshot.chain_bar_depth.get_num().to_string().as_str())
                        .block(Block::new().padding(Padding::new(
                            1, // left
                            0, // right
                            0, // top
                            0, // bottom
                        )))
                        .alignment(Alignment::Left),
                    progress_layout[2],
                );

                match date_string {
                    Some(date_string) => {
                        frame.render_widget(
                            Paragraph::new(date_string)
                                .block(Block::new().padding(Padding::new(
                                    1, // left
                                    0, // right
                                    0, // top
                                    0, // bottom
                                )))
                                .alignment(Alignment::Left),
                            progress_footer_layout[1],
                        );
                    }

                    None => {}
                }

                // frame.render_widget(
                //     Paragraph::new(format!(
                //         "{:?}\n{:?}\n{:?}",
                //         self.metrics_buffer.window_for_snapshot_prop("transactions"),
                //         self.metrics_buffer
                //             .window_for_snapshot_prop("blocks_processed"),
                //         self.metrics_buffer.timestamp_window(),
                //     )),
                //     layout[1],
                // );

                let chain_bar_progress_metrics = self
                    .metrics_buffer
                    .rates_for_snapshot_prop("blocks_processed");

                let time_remaining = match snapshot.chain_bar_depth.get_num() > 0
                    && snapshot.chain_bar_progress.get_num() > 0
                {
                    true => remaining_time(
                        chain_bar_progress_metrics.last().unwrap_or(&(0.0, 0.0)).1, // todo make 0 and handle
                        snapshot.chain_bar_depth.get_num() as i64,
                        snapshot.chain_bar_progress.get_num() as i64,
                    ),
                    false => 0.0.to_string(),
                };

                frame.render_widget(
                    Paragraph::new(format!("{} remaining", time_remaining))
                        .block(Block::new().padding(Padding::new(
                            0, // left
                            1, // right
                            0, // top
                            0, // bottom
                        )))
                        .alignment(Alignment::Right),
                    progress_footer_layout[2],
                );

                let chain_bar_window = self
                    .metrics_buffer
                    .window_for_snapshot_prop("blocks_processed");

                let time_window = self.metrics_buffer.timestamp_window();

                let transaction_metrics =
                    self.metrics_buffer.rates_for_snapshot_prop("transactions");
                let transaction_window =
                    self.metrics_buffer.window_for_snapshot_prop("transactions");

                let dataset_blocks = vec![Dataset::default()
                    .name("")
                    .marker(symbols::Marker::Braille)
                    .style(Style::default().fg(Color::Cyan))
                    .graph_type(GraphType::Line)
                    .data(&chain_bar_progress_metrics)];

                let dataset_txs = vec![Dataset::default()
                    .name("")
                    .marker(symbols::Marker::Braille)
                    .style(Style::default().fg(Color::Green))
                    .graph_type(GraphType::Line)
                    .data(&transaction_metrics)];

                // let y_max = chain_bar_window.1.max(transaction_window.1);
                // let y_min = chain_bar_window.1.min(transaction_window.1);
                // let y_avg = (y_min + y_max) / 2.0;

                // let y_max_s = y_max.round().to_string();
                // let y_avg_s = y_avg.round().to_string();
                // let y_min_s = y_min.round().to_string();

                let chain_bar_min_s = chain_bar_window.0.round().to_string();
                let chain_bar_max_s = chain_bar_window.1.round().to_string();
                let chain_bar_avg = (chain_bar_window.0.round() + chain_bar_window.1.round()) / 2.0;
                let chain_bar_avg_s = chain_bar_avg.round().to_string();

                let tx_min_s = transaction_window.0.round().to_string();
                let tx_max_s = transaction_window.1.round().to_string();
                let tx_avg = (transaction_window.0.round() + transaction_window.1.round()) / 2.0;
                let tx_avg_s = tx_avg.round().to_string();

                frame.render_widget(
                    Paragraph::new(format!("{}┈", chain_bar_max_s))
                        .block(Block::default().style(Style::default().fg(Color::Blue)))
                        .alignment(Alignment::Right),
                    chart_blocks_axis[0],
                );
                frame.render_widget(
                    Paragraph::new(format!("{}┈", chain_bar_avg_s))
                        .block(Block::default().style(Style::default().fg(Color::Blue)))
                        .alignment(Alignment::Right),
                    chart_blocks_axis[1],
                );
                frame.render_widget(
                    Paragraph::new(format!("{}┈", chain_bar_min_s))
                        .block(Block::default().style(Style::default().fg(Color::Blue)))
                        .alignment(Alignment::Right),
                    chart_blocks_axis[2],
                );

                frame.render_widget(
                    Paragraph::new(format!("┈{}", tx_max_s))
                        .block(Block::default().style(Style::default().fg(Color::Green)))
                        .alignment(Alignment::Left),
                    chart_tx_axis[0],
                );
                frame.render_widget(
                    Paragraph::new(format!("┈{}", tx_avg_s))
                        .block(Block::default().style(Style::default().fg(Color::Green)))
                        .alignment(Alignment::Left),
                    chart_tx_axis[1],
                );
                frame.render_widget(
                    Paragraph::new(format!("┈{}", tx_min_s))
                        .block(Block::default().style(Style::default().fg(Color::Green)))
                        .alignment(Alignment::Left),
                    chart_tx_axis[2],
                );

                let chart_legend_struts = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints(vec![
                        Constraint::Length(mixed_chart_layout[0].width + 1),
                        Constraint::Min(10),
                        Constraint::Length(mixed_chart_layout[2].width + 1),
                    ])
                    .split(layout[3]);

                let chart_legend_labels = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints(vec![
                        Constraint::Length(mixed_chart_layout[0].width + 8),
                        Constraint::Min(10),
                        Constraint::Length(mixed_chart_layout[2].width + 14),
                    ])
                    .split(layout[2]);

                frame.render_widget(
                    Paragraph::new("")
                        .block(
                            ratatui::widgets::Block::new()
                                .padding(Padding::new(
                                    0, // left
                                    0, // right
                                    0, // top
                                    0, // bottom
                                ))
                                .borders(Borders::RIGHT)
                                .border_style(Style::default().fg(Color::Blue)),
                        )
                        .alignment(Alignment::Right),
                    chart_legend_struts[0],
                );

                frame.render_widget(
                    Paragraph::new("")
                        .block(
                            ratatui::widgets::Block::new()
                                .padding(Padding::new(
                                    0, // left
                                    0, // right
                                    0, // top
                                    0, // bottom
                                ))
                                .borders(Borders::LEFT)
                                .border_style(Style::default().fg(Color::Green)),
                        )
                        .alignment(Alignment::Right),
                    chart_legend_struts[2],
                );

                frame.render_widget(
                    Paragraph::new("▄ Blocks")
                        .block(
                            ratatui::widgets::Block::new()
                                .padding(Padding::new(
                                    0, // left
                                    0, // right
                                    0, // top
                                    0, // bottom
                                ))
                                .fg(Color::Blue),
                        )
                        .alignment(Alignment::Right),
                    chart_legend_labels[0],
                );

                frame.render_widget(
                    Paragraph::new("Transactions ▄")
                        .block(
                            ratatui::widgets::Block::new()
                                .padding(Padding::new(
                                    0, // left
                                    0, // right
                                    0, // top
                                    0, // bottom
                                ))
                                .fg(Color::Green),
                        )
                        .alignment(Alignment::Left),
                    chart_legend_labels[2],
                );

                let chart = Chart::new(dataset_blocks)
                    .block(
                        Block::new()
                            .padding(Padding::new(
                                1, // left
                                0, // right
                                0, // top
                                0, // bottom
                            ))
                            .borders(Borders::RIGHT)
                            .border_style(Style::default().fg(Color::Green)),
                    )
                    .y_axis(Axis::default().bounds([chain_bar_window.0, chain_bar_window.1]))
                    .x_axis(Axis::default().bounds([time_window.0, time_window.1]));

                frame.render_widget(chart, mixed_chart_layout[1]);

                let chart2 = Chart::new(dataset_txs)
                    .block(
                        ratatui::widgets::Block::new()
                            .padding(Padding::new(
                                0, // left
                                1, // right
                                0, // top
                                0, // bottom
                            ))
                            .borders(Borders::LEFT)
                            .border_style(Style::default().fg(Color::Blue)),
                    )
                    .y_axis(Axis::default().bounds([transaction_window.0, transaction_window.1]))
                    .x_axis(Axis::default().bounds([time_window.0, time_window.1]));

                frame.render_widget(chart2, mixed_chart_layout[1]);

                // frame.render_widget(Paragraph::new("Bottom"), layout[0]);
                // frame.render_widget(Paragraph::new("Bottom"), layout[1]);
            })
            .unwrap();
    }

    async fn refresh(&mut self, pipeline: Option<&super::Pipeline>) -> Result<(), WorkerError> {
        let mut snapshot = MetricsSnapshot::default();
        snapshot.timestamp = Instant::now().duration_since(self.metrics_buffer.base_time);

        // match event::read() {
        //     Ok(event) => match event {
        //         crossterm::event::Event::Key(key) => match key.code {
        //             KeyCode::Char('q') => return Ok(()),
        //             _ => {}
        //         },

        //         _ => {}
        //     },
        //     _ => {}
        // }

        match pipeline {
            Some(pipeline) => {
                for tether in pipeline.tethers.iter() {
                    let state = match tether.check_state() {
                        TetherState::Dropped => "dropped!",
                        TetherState::Blocked(_) => "blocked",
                        TetherState::Alive(a) => match a {
                            StagePhase::Bootstrap => "⚠",
                            StagePhase::Working => "⚙",
                            StagePhase::Teardown => "⚠",
                            StagePhase::Ended => "ended",
                        },
                    };

                    if state == "blocked" {
                        log::warn!("{} is blocked", tether.name());
                    }

                    let tether_type: StageTypes = tether.name().into();

                    match tether_type {
                        StageTypes::Source => {
                            snapshot.sources_status = MeteredValue::Label(state.into())
                        }
                        StageTypes::Enrich => {
                            snapshot.enrich_status = MeteredValue::Label(state.into())
                        }
                        StageTypes::Reduce => {
                            snapshot.reducer_status = MeteredValue::Label(state.into())
                        }
                        StageTypes::Storage => {
                            snapshot.storage_status = MeteredValue::Label(state.into())
                        }
                        StageTypes::Unknown => {}
                    }

                    match tether.read_metrics() {
                        Ok(readings) => {
                            for (key, value) in readings {
                                match (tether.name(), key, value) {
                                    (_, "chain_tip", Reading::Gauge(x)) => {
                                        snapshot.chain_bar_depth.set_num(x as u64);
                                    }
                                    (_, "last_block", Reading::Gauge(x)) => {
                                        snapshot.chain_bar_progress.set_num(x as u64);
                                    }
                                    (_, "blocks_processed", Reading::Count(x)) => {
                                        snapshot.blocks_processed.set_num(x as u64);
                                    }
                                    // (_, "received_blocks", Reading::Count(x)) => {
                                    //     self.received_blocks.set_position(x);
                                    //     self.received_blocks.set_message(state);
                                    // }
                                    // (_, "ops_count", Reading::Count(x)) => {
                                    //     self.reducer_ops_count.set_position(x);
                                    //     self.reducer_ops_count.set_message(state);
                                    // }
                                    // (_, "reducer_errors", Reading::Count(x)) => {
                                    //     self.reducer_errors.set_position(x);
                                    //     self.reducer_errors.set_message(state);
                                    // }
                                    // (_, "storage_ops", Reading::Count(x)) => {
                                    //     self.storage_ops_count.set_position(x);
                                    //     self.storage_ops_count.set_message(state);
                                    // }
                                    (_, "transactions_finalized", Reading::Count(x)) => {
                                        snapshot.transactions.set_num(x as u64);
                                    }
                                    // (_, "enrich_cancelled_empty_tx", Reading::Count(x)) => {
                                    //     self.enrich_skipped_empty.set_position(x);
                                    //     self.enrich_skipped_empty.set_message(state);
                                    // }
                                    // (_, "enrich_removes", Reading::Count(x)) => {
                                    //     snapshot.enrich_hit.set_num(x);
                                    // }
                                    (_, "enrich_matches", Reading::Count(x)) => {
                                        snapshot.enrich_hit.set_num(x);
                                    }
                                    (_, "enrich_mismatches", Reading::Count(x)) => {
                                        snapshot.enrich_miss.set_num(x);
                                    }
                                    // (_, "enrich_blocks", Reading::Count(x)) => {
                                    //     self.enrich_blocks.set_position(x);
                                    //     self.enrich_blocks.set_message(state);
                                    // }
                                    // (_, "historic_blocks", Reading::Count(x)) => {
                                    //     self.historic_blocks.set_position(x);
                                    //     self.historic_blocks.set_message("");
                                    // }
                                    // (_, "historic_blocks_removed", Reading::Count(x)) => {
                                    //     self.historic_blocks_removed.set_position(x);
                                    //     self.historic_blocks_removed.set_message("");
                                    // }
                                    (_, "chain_era", Reading::Gauge(x)) => {
                                        if x > 0 {
                                            snapshot.chain_era.set_str(i64_to_string(x).as_str());
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        Err(_) => {
                            log::warn!("couldn't read metrics");
                        }
                    };
                }

                // Clean up

                self.draw(Some(pipeline.ctx.clone()), &snapshot).await;
                self.metrics_buffer.push(snapshot);
            }
            None => {
                self.draw(None, &snapshot).await;
            }
        }

        if event::poll(std::time::Duration::from_millis(16)).unwrap() {
            if let event::Event::Key(key) = event::read().unwrap() {
                if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                    stdout().execute(LeaveAlternateScreen).unwrap();
                    disable_raw_mode().unwrap();
                    return Err(WorkerError::Panic); // todo start shut down sequence
                }
            }
        }

        Ok(())
    }
}

impl Log for TuiConsole {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() >= log::Level::Info
    }

    fn log(&self, record: &log::Record) {
        let r = record.clone();
        let s = (r.level().to_string(), r.args().to_string());

        tokio::task::spawn(async move {
            let mut buffer = LOG_BUFFER.vec.lock().await;
            if buffer.len() == LOG_BUFFER.capacity {
                buffer.remove(0);
            }
            buffer.push(s);
        });
    }

    fn flush(&self) {}
}

struct PlainConsole {
    //last_report: Mutex<Instant>,
}

impl PlainConsole {
    fn new() -> Self {
        Self {
            //last_report: Mutex::new(Instant::now()),
        }
    }

    fn refresh(&self, pipeline: Option<&super::Pipeline>) -> Result<(), WorkerError> {
        match pipeline {
            Some(pipeline) => {
                for tether in pipeline.tethers.iter() {
                    match tether.check_state() {
                        gasket::runtime::TetherState::Dropped => {
                            log::error!("[{}] stage tether has been dropped", tether.name());
                        }
                        gasket::runtime::TetherState::Blocked(_) => {
                            log::warn!(
                                "[{}] stage tehter is blocked or not reporting state",
                                tether.name(),
                            );
                        }
                        gasket::runtime::TetherState::Alive(state) => {
                            log::debug!(
                                "[{}] stage is alive with state: {:?}",
                                tether.name(),
                                state
                            );
                            match tether.read_metrics() {
                                Ok(readings) => {
                                    for (key, value) in readings {
                                        log::debug!(
                                            "[{}] metric `{}` = {:?}",
                                            tether.name(),
                                            key,
                                            value
                                        );
                                    }
                                }
                                Err(err) => {
                                    log::error!(
                                        "[{}] error reading metrics: {}",
                                        tether.name(),
                                        err
                                    )
                                }
                            }
                        }
                    }
                }
            }
            None => {}
        }

        Ok(())
    }
}

impl log::Log for PlainConsole {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        eprintln!("{}", record.args())
    }

    fn flush(&self) {}
}

enum Logger {
    Tui(TuiConsole),
    Plain(PlainConsole),
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        match self {
            Self::Tui(tui_console) => tui_console.enabled(metadata),
            Self::Plain(plain_console) => plain_console.enabled(metadata),
        }
    }

    fn log(&self, record: &log::Record) {
        match self {
            Self::Tui(tui_console) => tui_console.log(record),
            Self::Plain(plain_console) => plain_console.log(record),
        }
    }

    fn flush(&self) {
        match self {
            Self::Tui(tui_console) => tui_console.flush(),
            Self::Plain(_plain_console) => {}
        }
    }
}

lazy_static! {
    static ref TUI_CONSOLE: Mutex<TuiConsole> = Mutex::new(TuiConsole::new());
}

lazy_static! {
    static ref PLAIN_CONSOLE: PlainConsole = PlainConsole::new();
}

lazy_static! {
    static ref LOG_BUFFER: LogBuffer = LogBuffer {
        vec: Arc::new(Mutex::new(Vec::new())),
        capacity: 100,
    };
}
pub async fn initialize(mode: &Mode) {
    let logger = match mode {
        Mode::TUI => Logger::Tui(TuiConsole::new()),
        _ => Logger::Plain(PlainConsole::new()),
    };

    log::set_boxed_logger(Box::new(logger))
        .map(|_| log::set_max_level(log::LevelFilter::Info))
        .unwrap();
}

pub async fn refresh(mode: &Mode, pipeline: Option<&super::Pipeline>) -> Result<(), WorkerError> {
    let mode = match mode {
        Mode::TUI => TUI_CONSOLE.lock().await.refresh(pipeline).await,
        _ => PLAIN_CONSOLE.refresh(pipeline),
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    mode
}
