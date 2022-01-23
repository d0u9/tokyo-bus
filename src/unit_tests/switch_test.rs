// This test file is references directly via
// #[path = "unit_tests/switch_test.rs"]
// from switch.rs
//
// use std::mem::drop;

use claim::assert_ok;
use claim::assert_err;
use test_log::test;
use tokio::time::{self, Duration};

use super::super::packet_endpoint::PktWire;
use super::*;

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

#[test(tokio::test)]
async fn test_switch_add_endpoint_via_handler() {
    let switch = Switch::<u32>::new();

    let (server, switch_handler) = assert_ok!(switch.server());
    let handler = tokio::spawn(async move {
        server.serve().await;
    });

    let this_addr = Address::new("this");
    let that_addr = Address::new("that");

    time::sleep(Duration::from_millis(5)).await;
    let ep0 = assert_ok!(switch_handler.new_endpoint(this_addr.clone()).await);
    let ep1 = assert_ok!(switch_handler.new_endpoint(that_addr.clone()).await);

    let (tx0, _) = assert_ok!(ep0.split());
    let (_, mut rx1) = assert_ok!(ep1.split());

    let val = 66666666_u32;
    assert_ok!(tx0.send_data(&that_addr, val));

    let echo_val = assert_ok!(rx1.recv_data().await);

    assert_eq!(val, echo_val);

    println!("send shutdown signal");
    assert_ok!(switch_handler.shutdown_server());

    assert_ok!(handler.await);
}

#[test(tokio::test)]
async fn test_switch_close_endpoints_when_shutdown() {
    let switch = Switch::<u32>::new();

    let (server, switch_handler) = assert_ok!(switch.server());
    let handler = tokio::spawn(async move {
        server.serve().await;
    });

    let this_addr = Address::new("this");
    let that_addr = Address::new("that");

    time::sleep(Duration::from_millis(5)).await;
    let ep0 = assert_ok!(switch_handler.new_endpoint(this_addr.clone()).await);
    let ep1 = assert_ok!(switch_handler.new_endpoint(that_addr.clone()).await);

    let val = 66666666_u32;

    let (tx0, _) = assert_ok!(ep0.split());
    let handler2 = tokio::spawn(async move {
        let (_, mut rx1) = assert_ok!(ep1.split());
        let echo_val = assert_ok!(rx1.recv_data().await);
        assert_eq!(val, echo_val);

        assert_err!(rx1.recv_data().await);
    });

    let val = 66666666_u32;
    assert_ok!(tx0.send_data(&that_addr, val));

    time::sleep(Duration::from_millis(5)).await;
    println!("send shutdown signal");
    assert_ok!(switch_handler.shutdown_server());

    assert_ok!(handler.await);
    assert_ok!(handler2.await);
}
