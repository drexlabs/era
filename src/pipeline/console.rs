use chrono::Duration as ChronoDuration;
use chrono::{DateTime, Utc};
use crossterm::execute;
use gasket::framework::{AsWorkError, WorkSchedule};
use gasket::messaging::{InputPort, OutputPort};
use gasket::runtime::Policy;
use gasket::{
    framework::WorkerError,
    metrics::Reading,
    runtime::{StagePhase, TetherState},
};
use gasket_log::model::Log;
use ratatui::prelude::Layout;
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Padding};
use serde::Deserialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::crosscut;

use crate::model::{
    merge_metrics_snapshots, MeteredNumber, MeteredString, MeteredValue, MetricsSnapshot,
    ProgramOutput,
};

use super::{Context, Pipeline, StageTypes};

use crossterm::{
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{prelude::*, widgets::Paragraph};
use std::io::{stdout, Stdout};

pub struct FrameState {
    last_frame: Instant,
    frame_times: VecDeque<Duration>,
    target_fps: u32,
    sample_window: usize,
}

impl FrameState {
    pub fn new(target_fps: u32) -> Self {
        Self {
            last_frame: Instant::now(),
            frame_times: VecDeque::with_capacity(60), // 1 second at 60fps
            target_fps,
            sample_window: 60,
        }
    }

    pub fn record_frame(&mut self) {
        let now = Instant::now();
        let frame_time = now.duration_since(self.last_frame);

        if self.frame_times.len() >= self.sample_window {
            self.frame_times.pop_front();
        }

        self.frame_times.push_back(frame_time);

        self.last_frame = now;
    }

    pub fn get_target_duration(&self) -> Duration {
        Duration::from_secs_f64(1.0 / self.target_fps as f64)
    }

    pub fn should_render(&self) -> bool {
        let now = Instant::now();
        now.duration_since(self.last_frame) >= self.get_target_duration()
    }

    // Adapts FPS based on actual performance
    pub fn adapt_fps(&mut self) {
        if self.frame_times.len() < 30 {
            return; // Need enough samples
        }

        let avg_frame_time: Duration =
            self.frame_times.iter().sum::<Duration>() / self.frame_times.len() as u32;
        let current_fps = 1.0 / avg_frame_time.as_secs_f64();

        // If we're consistently taking longer than our frame budget, reduce target FPS
        if current_fps < (self.target_fps as f64 * 0.9) {
            self.target_fps = (self.target_fps * 2 / 3).max(30);
        }
        // If we have headroom, try to increase FPS
        else if current_fps > (self.target_fps as f64 * 1.1) {
            self.target_fps = (self.target_fps * 3 / 2).min(60);
        }
    }
}

pub struct Bootstrapper {
    stage: Stage,
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<ProgramOutput> {
        &mut self.stage.input
    }

    pub fn spawn_stage(self, policy: Policy) -> gasket::runtime::Tether {
        gasket::runtime::spawn_stage(self.stage, policy)
    }
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub mode: Mode,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Default::default(),
        }
    }
}

impl Config {
    pub fn bootstrapper(self, ctx: Arc<Context>) -> Bootstrapper {
        let stage = Stage {
            mode: self.mode,
            input: Default::default(),
            frames_rendered: Default::default(),
            frame_time: Default::default(),
            metrics_snapshot_totality: Default::default(),
            visual_log_buffer: Default::default(),
            ctx,
        };

        Bootstrapper { stage }
    }
}

pub struct ConsoleWorker {
    metrics_buffer: BlockGraph,
    frame_state: FrameState,
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl ConsoleWorker {
    async fn draw(
        &mut self,
        ctx: Arc<Context>,
        snapshot: &MetricsSnapshot,
        visual_log_buffer: &VecDeque<Log>,
    ) {
        let current_era = snapshot.chain_era.clone().unwrap().get_string();

        let provider = crosscut::time::NaiveProvider::new(ctx).await;
        let wallclock = provider.slot_to_wallclock(
            snapshot
                .chain_bar_progress
                .clone()
                .unwrap_or(MeteredValue::Numerical(MeteredNumber::default()))
                .get_num(),
        );
        let d = SystemTime::UNIX_EPOCH + Duration::from_secs(wallclock);
        let datetime = DateTime::<Utc>::from(d);
        let date_string = Some(datetime.format("%Y-%m-%d %H:%M:%S").to_string());

        let frame = self.terminal.get_frame();

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

        let calc = if layout[1].height as usize >= 2 {
            layout[1].height as usize - 2
        } else {
            0
        };

        self.terminal
            .draw(|frame| {
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
                        match snapshot.chain_bar_depth.clone().unwrap_or(MeteredValue::Numerical(MeteredNumber::default())).get_num() > 0
                        {
                            true => ((snapshot.chain_bar_progress.clone().unwrap_or(crate::model::MeteredValue::Numerical(MeteredNumber::default()))).get_num() as f64
                                / (snapshot.chain_bar_depth.clone().unwrap_or(crate::model::MeteredValue::Numerical(MeteredNumber::default()))).get_num() as f64
                                * 100.0)
                                .round() as u16,
                            false => 0,
                        },
                    );

                //let bottom_pane = ratatui::widgets::Block::default().title("Hi");

                //frame.render_widget(bottom_pane, layout[0]);

                let log_lines: Vec<Line> = visual_log_buffer
                    .iter()
                    .rev()
                    .take(calc)
                    .map(|entry| Line::from(entry.to_string()))
                    .collect();
                
                frame.render_widget(
                    Paragraph::new(Text::from(log_lines))
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
                    Paragraph::new(snapshot.chain_era.clone().unwrap_or(MeteredValue::Label(MeteredString::default())).get_string())
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

                let status_sublayout = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints(vec![
                        Constraint::Length(2),
                        Constraint::Length(1),
                        Constraint::Min(1),
                    ])
                    .split(top_status_layout[1]);
                
                let sources_status = snapshot.sources_status.clone().unwrap_or(MeteredValue::Label(MeteredString::default())).get_string();
                let enrich_status = snapshot.enrich_status.clone().unwrap_or(MeteredValue::Label(MeteredString::default())).get_string();
                let reducer_status = snapshot.reducer_status.clone().unwrap_or(MeteredValue::Label(MeteredString::default())).get_string();
                let storage_status = snapshot.storage_status.clone().unwrap_or(MeteredValue::Label(MeteredString::default())).get_string();
                
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
                        "{} Source ({} blocks)\n{} Enrich ({} hits / {} misses)\n{} Reduce\n{} Storage",
                        if sources_status.is_empty() {
                            "⧗".to_string()
                        } else {
                            sources_status
                        },
                        snapshot.blocks_ingested.clone().unwrap_or(MeteredValue::Numerical(MeteredNumber::default())).get_num(),
                        if enrich_status.is_empty() {
                            "⚠".to_string()
                        } else {
                            enrich_status
                        },
                        snapshot.enrich_hit.clone().unwrap_or(MeteredValue::Numerical(MeteredNumber::default())).get_num(),
                        snapshot.enrich_miss.clone().unwrap_or(MeteredValue::Numerical(MeteredNumber::default())).get_num(),
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
                    Paragraph::new(snapshot.chain_bar_depth.clone().unwrap_or(MeteredValue::Numerical(MeteredNumber::default())).get_num().to_string().as_str())
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

                let time_remaining = remaining_time(
                    chain_bar_progress_metrics.last().unwrap_or(&(0.0, 0.0)).1, // todo make 0 and handle
                    snapshot.chain_bar_depth.clone().unwrap_or(MeteredValue::Numerical(MeteredNumber::default())).get_num() as i64,
                    snapshot.chain_bar_progress.clone().unwrap_or(MeteredValue::Numerical(MeteredNumber::default())).get_num() as i64,
                );

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
}

#[derive(gasket::framework::Stage)]
#[stage(name = "console-renderer", unit = "()", worker = "ConsoleWorker")]
pub struct Stage {
    pub mode: Mode,
    pub input: InputPort<ProgramOutput>,
    pub metrics_snapshot_totality: MetricsSnapshot,
    pub ctx: Arc<Context>,
    pub visual_log_buffer: VecDeque<Log>,

    #[metric]
    frames_rendered: gasket::metrics::Counter,
    #[metric]
    frame_time: gasket::metrics::Gauge,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for ConsoleWorker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        if let Mode::TUI = stage.mode {
            stdout().execute(EnterAlternateScreen).unwrap();
            enable_raw_mode().unwrap();
        }

        Ok(Self {
            metrics_buffer: BlockGraph::new(RING_DEPTH),
            frame_state: FrameState::new(60),
            terminal: Terminal::new(CrosstermBackend::new(stdout())).unwrap(),
        })
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        match stage
            .input
            .recv()
            .await
            .map_err(|_| WorkerError::Recv)
            .map(|u| u.payload)?
        {
            ProgramOutput::Log(logs) => {
                for item in logs {
                    stage.visual_log_buffer.push_back(item);
                    if stage.visual_log_buffer.len() > RING_DEPTH {
                        stage.visual_log_buffer.pop_front();
                    }
                }
            }

            ProgramOutput::Metrics(metrics) => {
                for item in metrics {
                    stage.metrics_snapshot_totality.merge(&item);

                    self.metrics_buffer.push(item);
                }
            }
        };

        
        
        if self.frame_state.should_render() {
            Ok(WorkSchedule::Unit(()))
        } else {
            let is_tui = match stage.mode {
                Mode::Plain => false,
                Mode::TUI => true,
            };

            if is_tui {
                Ok(WorkSchedule::Idle)
            } else {
                Ok(WorkSchedule::Unit(()))
            }
            
            
        }
    }

    async fn execute(&mut self, _unit: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        let start = Instant::now();

        match stage.mode {
            Mode::TUI => {
                self.draw(
                    Arc::clone(&stage.ctx),
                    &stage.metrics_snapshot_totality,
                    &stage.visual_log_buffer,
                )
                .await;

                self.metrics_buffer
                    .push(stage.metrics_snapshot_totality.clone());
            }
            Mode::Plain => {
                // Plain console logic
                let terminal = self.terminal.backend_mut();
                while let Some(log) = stage.visual_log_buffer.pop_front() {
                    execute!(terminal, crossterm::style::Print(format!("{}\n", log)))
                        .map_err(|_| WorkerError::Retry)?;
                }
            }
        };

        self.frame_state.record_frame();
        self.frame_state.adapt_fps();

        stage.frame_time.set(start.elapsed().as_micros() as i64);
        stage.frames_rendered.inc(1);

        Ok(())
    }
}

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

#[derive(clap::ValueEnum, Deserialize, Clone)]
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

pub struct BlockGraph {
    vec: VecDeque<MetricsSnapshot>,
    base_time: Instant,
    capacity: usize,
    last_dropped: Option<MetricsSnapshot>,
}

impl BlockGraph {
    pub fn new(capacity: usize) -> Self {
        let base_time = Instant::now();
        let vec: VecDeque<MetricsSnapshot> = Default::default();

        Self {
            vec,
            base_time,
            capacity,
            last_dropped: None,
        }
    }

    pub fn push(&mut self, ele: MetricsSnapshot) {
        if self.vec.len() == self.capacity {
            self.last_dropped = self.vec.pop_front();
        }
        self.vec.push_back(ele);
    }

    pub fn get(&self, index: usize) -> &MetricsSnapshot {
        self.vec.get(index).unwrap()
    }

    pub fn timestamp_window(&self) -> (f64, f64) {
        let mut min: Duration = Default::default();
        let mut max: Duration = Default::default();

        for snapshot in self.vec.clone() {
            let current = snapshot.timestamp.unwrap_or_default();

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
                .unwrap_or_default()
                .checked_sub(Duration::from_millis(1000)),
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
                    .unwrap_or(MeteredValue::Numerical(MeteredNumber::default()))
                    .get_num()
            } else {
                previous_snapshot
                    .get_metrics_key(prop_name)
                    .map(|v| v.get_num())
                    .unwrap_or(0)
            };

            let current_value = self
                .get_prop_value_for_index(prop_name, i)
                .map(|v| v.get_num())
                .unwrap_or(0);

            let time_diff = if current_duration > previous_duration {
                (current_duration.unwrap() - previous_duration.unwrap_or_default()).as_secs_f64()
            } else {
                0.0
            };

            let value_diff = match (current_value, previous_value) {
                (0, _) => 0,
                (_, 0) => current_value,
                (curr, prev) => curr - prev,
            };

            let rate_of_increase = if time_diff > 0.0 && value_diff > 0 {
                value_diff as f64 / time_diff
            } else {
                0.0
            };

            rates.push((
                current_snapshot.timestamp.unwrap_or_default().as_secs_f64(),
                rate_of_increase,
            ));
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

// impl Deref for TuiConsole {
//     type Target = Terminal<CrosstermBackend<std::io::Stdout>>;

//     fn deref(&self) -> &Self::Target {
//         &self.terminal
//     }
// }

// impl DerefMut for TuiConsole {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.terminal
//     }
// }

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

const RING_DEPTH: usize = 10;
