# sqlite-es

> An SQLite implementation of the `PersistedEventRepository` trait in cqrs-es.

---

## Usage
Add to your Cargo.toml file:

```toml
[dependencies]
cqrs-es = "0.4.5"
sqlite-es = "0.4.5"
```

Requires access to a SQLite DB with existing tables. See:
- [Sample database configuration](db/init.sql)

A simple configuration example:
```
let store = default_sqlite_pool("postgresql://my_user:my_pass@localhost:5432/my_db");
let cqrs = sqlite_es::postgres_cqrs(pool, vec![])
```

Things that could be helpful:
- [User guide](https://doc.rust-cqrs.org) along with an introduction to CQRS and event sourcing.
- [Demo application](https://github.com/johnbcodes/cqrs-demo-sqlite) using the warp http server.
- [Change log](https://github.com/serverlesstechnology/cqrs/blob/master/docs/versions/change_log.md)

[//]: # ([![Crates.io]&#40;https://img.shields.io/crates/v/sqlite-es&#41;]&#40;https://crates.io/crates/sqlite-es&#41;)
[//]: # ([![docs]&#40;https://img.shields.io/badge/API-docs-blue.svg&#41;]&#40;https://docs.rs/postgres-es&#41;)
[//]: # (![docs]&#40;https://codebuild.us-west-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiVVUyR0tRbTZmejFBYURoTHdpR3FnSUFqKzFVZE9JNW5haDZhcUFlY2xtREhtaVVJMWsxcWZOeC8zSUR0UWhpaWZMa0ZQSHlEYjg0N2FoU2lwV1FsTXFRPSIsIml2UGFyYW1ldGVyU3BlYyI6IldjUVMzVEpKN1V3aWxXWGUiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master&#41;)
