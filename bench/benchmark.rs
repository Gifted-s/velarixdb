use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use velarixdb::db::DataStore;


fn insert(c: &mut Criterion) {
    let root = tempdir().unwrap();
    let path = root.path().join("default");
    let runtime = Runtime::new().unwrap();
    let store = Arc::new(RwLock::new(
        runtime.block_on(async { DataStore::open("benchmark", path).await.unwrap() }),
    ));
    c.bench_function("put", |b| {
        b.to_async(&runtime).iter(|| async {
            let key = b"key";
            let value = b"value";
            store.write().await.put(&key, &value).await.unwrap();
        });
    });
}

fn get_many(c: &mut Criterion) {
    let root = tempdir().unwrap();
    let path = root.path().join("default");
    let runtime = Runtime::new().unwrap();
    let store = Arc::new(RwLock::new(
        runtime.block_on(async { DataStore::open("benchmark", path).await.unwrap() }),
    ));

    runtime.block_on(async {
        for e in 1..=20000 {
            let e = e.to_string();
            store.write().await.put(&e, &e).await.unwrap();
        }
    });

    c.bench_function("get_many", |b| {
        b.to_async(&runtime).iter(|| async {
            for e in 1..=20000 {
                let e = e.to_string();
                store.read().await.get(&e).await.unwrap();
            }
        });
    });
}


fn put_many(c: &mut Criterion) {
    let root = tempdir().unwrap();
    let path = root.path().join("default");
    let runtime = Runtime::new().unwrap();
    let store = Arc::new(RwLock::new(
        runtime.block_on(async { DataStore::open("benchmark", path).await.unwrap() }),
    ));

    c.bench_function("put_many", |b| {
        b.to_async(&runtime).iter(|| async {
            for e in 1..=20000 {
                let e = e.to_string();
                store.write().await.put(&e, &e).await.unwrap();
            }
        });
    });
}

criterion_group!(benches, insert, get_many, put_many);
criterion_main!(benches);
