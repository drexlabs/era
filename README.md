<div align="center">
    <img src="./assets/128x128@2x.png" alt="Era Logo" width="256">
    <hr />
        <h3 align="center" style="border-bottom: none">Read-optimized cache of Cardano on-chain entities</h3>
        <img alt="GitHub" src="https://img.shields.io/github/license/mkeen/era" />
    <hr/>
</div>

## Intro

_Era_ indexes Cardano's blockchain data in a high-level fashion and in a way that ensures you can effectively fetch or subscribe to whatever data you need in real-time.

The focus is on providing a high level index of data that is useful for building consumer-facing experiences.

> _Era_ is a fork of _Scrolls_. Significant differences as of now are real-time support for rollbacks via both N2C and N2N protocols, a refocused reducer-set, and a systemd/Debian-first posture. Support for Docker will be reintroduced in the next couple waves of updates.

This project serves as the index-hydrator for my own projects, and will be updated as contributers and myself see fit. Doesn't do something you need? Dig in and open a PR, or create an issue that describes your feature request.

## Building & Installing (Debian)

Make sure you have `cargo` installed so that you can build the project. If you can run Cargo, you can run the compiled binary.

1. `cargo build --all-features --release` (release arg is optional)

[Optional]
2. `cargo deb`
3. `dpkg -i ...`
4. `systemctl enable era` (enable start on boot)

## Start with cli

`RUST_LOG="warn" /usr/bin/era daemon --config /etc/era/config.toml --console tui`

## Start with systemd

If you installed with dpkg, you can start and stream logs with these

`systemctl start era`

## Some useful dev commands I use that you can repurpose:

~/Services/cardano/cardano-node-8.7.3-linux/cardano-node run --socket-path /tmp/node.socket --config ~/Services/cardano/cardano-node-8.7.3-linux/configuration/cardano/mainnet-config.json --topology ~/Services/cardano/cardano-node-8.7.3-linux/configuration/cardano/mainnet-topology.json --database-path ~/Services/cardano/cardano-node-8.7.3-linux/mainnet/db

