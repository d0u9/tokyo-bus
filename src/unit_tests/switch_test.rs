// This test file is references directly via
// #[path = "unit_tests/switch_test.rs"]
// from switch.rs
//
// use std::mem::drop;

use test_log::test;
use claim::assert_err;
use claim::assert_ok;
use tokio::time::{self, Duration};


use super::*;
use super::super::packet_endpoint::PktWire;

#[test(tokio::test)]
async fn test_switch_no_ports() {
    let switch = Switch::<u32>::new();

    let (server, switch_handler) = assert_ok!(switch.server());
    let handler = tokio::spawn(async move {
        server.serve().await;
    });

    time::sleep(Duration::from_millis(5)).await;

    println!("send shutdown signal");
    assert_ok!(switch_handler.shutdown_server());

    assert_ok!(handler.await);
}

#[test(tokio::test)]
async fn test_switch_send_to_self() {
    let this_addr = Address::random();
    let mut switch = Switch::<u32>::new();
    let (ep0, ep1) = PktWire::endpoints(this_addr.clone());

    assert_ok!(switch.attach_endpoint(ep0));
    let (server, switch_handler) = assert_ok!(switch.server());

    let handler = tokio::spawn(async move {
        server.serve().await;
    });

    time::sleep(Duration::from_millis(5)).await;

    let (tx, mut rx) = assert_ok!(ep1.split());

    let val = 11111111_u32;
    assert_ok!(tx.send_data(&this_addr, val));
    let recv_val = assert_ok!(rx.recv_data().await);
    assert_eq!(val, recv_val);

    time::sleep(Duration::from_millis(5)).await;

    println!("send shutdown signal");
    assert_ok!(switch_handler.shutdown_server());

    assert_ok!(handler.await);
}
