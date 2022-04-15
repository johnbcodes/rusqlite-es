# postgres-es

> A Postgres implementation of the `PersistedEventRepository` trait in cqrs-es.

---

## Usage
Add to your Cargo.toml file:

```toml
[dependencies]
cqrs-es = "0.3.1"
postgres-es = "0.3.1"
```

Requires access to a Postgres DB with existing tables. See:
- [Sample database configuration](db/init.sql)
- Use `docker-compose` to quickly setup [a local database](docker-compose.yml)

A simple configuration example:
```
let store = default_postgress_pool("postgresql://my_user:my_pass@localhost:5432/my_db");
let cqrs = postgres_es::postgres_cqrs(pool, vec![])
```

Things that could be helpful:
- [User guide](https://doc.rust-cqrs.org) along with an introduction to CQRS and event sourcing.
- [Demo application](https://github.com/serverlesstechnology/cqrs-demo) using the warp http server.
- [Change log](https://github.com/serverlesstechnology/cqrs/blob/master/docs/versions/change_log.md)

[![Crates.io](https://img.shields.io/crates/v/postgres-es)](https://crates.io/crates/postgres-es)
[![docs](https://img.shields.io/badge/API-docs-blue.svg)](https://docs.rs/postgres-es)
