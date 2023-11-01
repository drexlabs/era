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

## Redis

The entire history of everything that's ever happened on-chain since Cardano got its start will be loaded into Redis. N2N client is limited by the N2N protocol APIs.. and takes about a week to fully sync. UTXORPC protocol is much faster and full-featured. Client is stubbed out here in the repo, but is not functional yet.

Once the chain is synced, it will remain synced, fully in real-time, with absolutely no validation. Rollbacks are handled by Era. Keep this in mind when you see data flappage when subscribing to real-time updates from Redis. It is normal to see transactions rolled back -- this is typically called a double spend -- but it happens for many reasons. You may want to handle this accordingly in your API, but you can get by without doing that.

### Accessing Reducer Aggregate Data

#### Ada Handles

Handle data is stored in a reversable index, so you can look up the SOA of a handle, or the handle of a SOA with a single, cheap lookup each way.

```
> GET h.cardopoly
> stake1uycm70run8hcwn6uzqj5dnwtk68nuxg4pf37mlhr6enkpqg432rcr
```

```
> GET h.stake1uycm70run8hcwn6uzqj5dnwtk68nuxg4pf37mlhr6enkpqg432rcr
> cardopoly
```

#### Asset Metadata

CIP-25 and CIP-27 related metadata are both available. For CIP-25, the metadata is stored in a sorted set, sorted by the timestamp of asset minting. This allows you to easily look up the most recent CIP-25 metadata for any asset on the network -- as well as the CIP-27 transactions for any policy.

##### Look up asset metadata by asset id
```
> GET m.asset17aapep2mau8dfmvvttus0yv7t4kxfpzuleg5yk
> {"721": ...}
```

##### Look up royalty metadata by policy id
```
> GET m.285c0b8e91ba323da4ca083c9db837e111dafbf3143ece4d03eba8f4
> {"721": ...}
```



## Debian/Systemd

Releases are in the form of .deb files and accompanied by source code. Installs systemd service, and the release binary and default config.

## SOAs

Era organizes information about asset/utxo ownership using SOAs. SOA stands for "Stake or Address". The concept behind this is simple: If a Cardano user has a stake key associated with their address, we will store information about what they own indexed by that stake key, in order to reduce keyspace size, and to keep queries for things like wallet UI population light. If there is no stake key associated with an address, Era will index on address. Hence: SOA.

