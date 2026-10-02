use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rust_engineio::{Packet, PacketId};
use std::hint::black_box;

fn codec(c: &mut Criterion) {
    for id in [PacketId::Message, PacketId::MessageBinary] {
        let mut group = c.benchmark_group(format!("encode/{id:?}"));
        for size in [32, 1024, 65536] {
            let packet = Packet::new(id, Bytes::from(vec![b'x'; size]));
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), &packet, |b, packet| {
                b.iter(|| Bytes::from(black_box(packet.clone())));
            });
        }
        group.finish();
    }
}

criterion_group!(benches, codec);
criterion_main!(benches);
