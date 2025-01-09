use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;
use tokio::runtime;
use velarixdb::db::DataStore;

// Not sure if this is the best way to implement benchmark for async ops but
// for now it LGTM (logically), review is accepted!
fn insert_many(c: &mut Criterion) {
    let root = tempdir().unwrap();
    let path = root.path().join("default");
    let rt = runtime::Runtime::new().unwrap();
    let mut store = { rt.block_on(async { DataStore::open("benchmark", path).await.unwrap() }) };
    c.bench_function("insert_many", |b| {
        b.iter(|| {
            rt.block_on(async {
                for e in 1..=3000 {
                    let e = e.to_string();
                    store.put(&e, &e).await.unwrap();
                }
            });
        });
    });
}

fn get_many(c: &mut Criterion) {
    let root = tempdir().unwrap();
    let path = root.path().join("default");
    let rt = runtime::Runtime::new().unwrap();
    let mut store = { rt.block_on(async { DataStore::open("benchmark", path).await.unwrap() }) };

    rt.block_on(async {
        for e in 1..=3000 {
            let e = e.to_string();
            store.put(&e, &e).await.unwrap();
        }
    });

    c.bench_function("get_many", |b| {
        b.iter(|| {
            rt.block_on(async {
                for e in 1..=3000 {
                    let e = e.to_string();
                    store.get(&e).await.unwrap();
                }
            });
        });
    });
}

criterion_group!(benches, insert_many, get_many);
criterion_main!(benches);
