use cassandra_protocol::frame::frame_result::{ResResultBody, ResultKind};
use cassandra_protocol::frame::FromCursor;
use cassandra_protocol::{
    frame::Serialize,
    types::{to_int, CInt},
};
use criterion::{criterion_group, criterion_main, Criterion};

//use bytes::BytesMut;
// use scylla::frame::types;

fn types_benchmark(c: &mut Criterion) {
    let mut buf: Vec<u8> = Vec::with_capacity(100);
    let mut cursor = std::io::Cursor::new(&mut buf);
    let i = to_int(-1);
    // c.bench_function("short", |b| {
    //     b.iter(|| {
    //         buf.clear();
    //         types::write_short(-1, &mut buf);
    //         types::read_short(&mut &buf[..]).unwrap();
    //     })
    // });
    c.bench_function("int", |b| {
        b.iter(|| {
            i.serialize(&mut cursor);
        })
    });

    c.bench_function("b", |b| {
        let mut body: &[u8] = &[
            0, 0, 0, 4, // prepared flags
            0, 2, 0, 1, // id
            //
            // prepared flags
            0, 0, 0, 1, // global table space flag
            0, 0, 0, 3, // columns counts
            0, 0, 0, 1, // pk_count
            0, 0, // pk_index
            //
            // col specs
            // col spec 1
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 102, 111, 111, // foo
            0, 9, // id
            //
            // col spec 2
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 98, 97, 114, // bar
            0, 19, // id
            //
            // rows metadata
            0, 0, 0, 0, // empty flags
            0, 0, 0, 3, // columns count
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 102, 111, 111, // foo
            0, 9, // int
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 98, 97, 114, // bar
            0, 19, // id
        ];
        let mut cursor = std::io::Cursor::new(body);
        b.iter(|| {
            let res = ResResultBody::parse_body_from_cursor(&mut cursor, ResultKind::Prepared);
        });
    });
    //     c.bench_function("long", |b| {
    //         b.iter(|| {
    //             buf.clear();
    //             types::write_long(-1, &mut buf);
    //             types::read_long(&mut &buf[..]).unwrap();
    //         })
    //     });
    //     c.bench_function("string", |b| {
    //         b.iter(|| {
    //             buf.clear();
    //             types::write_string("hello, world", &mut buf);
    //             types::read_string(&mut &buf[..]).unwrap();
    //         })
    //     });
}

criterion_group!(benches, types_benchmark);
criterion_main!(benches);
