// This test file is references directly via
// #[path = "unit_tests/wire_test.rs"]
// from wire.rs
use std::mem::drop;

use claim::assert_err;
use claim::assert_ok;
use test_log::test;
use tokio::time::{self, Duration};

use super::*;

#[test(tokio::test)]
async fn test_wire_endpoint_counter() {
    let ep0 = {
        let (ep0, _ep1) = Wire::<u32>::endpoints();
        let wire_ref = ep0.wire_ref();

        assert_eq!(wire_ref.get_peer_counter(), (1, 1));
        assert_eq!(Arc::strong_count(wire_ref), 2);

        ep0
    };

    let wire = ep0.wire();
    assert_eq!(wire.get_peer_counter(), (1, 0));
    assert_eq!(Arc::strong_count(&wire), 2);

    drop(ep0);

    assert_eq!(wire.get_peer_counter(), (0, 0));
    assert_eq!(Arc::strong_count(&wire), 1);
}

#[test(tokio::test)]
async fn test_wire_endpoint_counter_after_split() {
    let (ep0, ep1) = Wire::<u32>::endpoints();

    let wire = ep0.wire();

    let (tx_ep0, rx_ep0) = assert_ok!(ep0.split());
    let (tx_ep1, rx_ep1) = assert_ok!(ep1.split());

    assert_eq!(wire.get_peer_counter(), (1, 1));

    drop(tx_ep0);
    drop(rx_ep1);

    assert_eq!(wire.get_peer_counter(), (1, 1));

    drop(rx_ep0);
    assert_eq!(wire.get_peer_counter(), (0, 1));

    drop(tx_ep1);
    assert_eq!(wire.get_peer_counter(), (0, 0));
}

#[test(tokio::test)]
async fn test_wire_simplex() {
    let (ep0, ep1) = Wire::<u32>::endpoints();
    let (tx, _) = assert_ok!(ep0.split());
    let (_, mut rx) = assert_ok!(ep1.split());

    let test_val = 0xdeadbeef_u32;

    let handler = tokio::spawn(async move {
        let val = assert_ok!(rx.recv().await);
        assert_eq!(val, test_val);
    });

    time::sleep(Duration::from_millis(20)).await;
    assert_ok!(tx.send(test_val));

    assert_ok!(handler.await);
}

#[test(tokio::test)]
async fn test_wire_one_side_all_droped() {
    let (ep0, ep1) = Wire::<u32>::endpoints();

    let handler = tokio::spawn(async move {
        let ep1_fork = ep1.clone();
        let (_, mut rx) = assert_ok!(ep1_fork.split());

        dbg!(assert_err!(rx.recv().await));
    });

    // Wait for recv being splited.
    time::sleep(Duration::from_millis(10)).await;

    drop(ep0);

    assert_ok!(handler.await);
}

#[test(tokio::test)]
async fn test_wire_split_dead_endpoint() {
    let (ep0, ep1) = Wire::<u32>::endpoints();

    let handler = tokio::spawn(async move {
        // wait for peer be dropped
        time::sleep(Duration::from_millis(10)).await;
        dbg!(assert_err!(ep1.split()));
    });

    drop(ep0);

    assert_ok!(handler.await);
}

#[test(tokio::test)]
async fn test_wire_split_after_another_peer_reopend() {
    let (ep0, ep1) = Wire::<u32>::endpoints();

    let ep1_fork = ep1.clone();

    let handler = tokio::spawn(async move {
        let ep1 = ep1_fork.clone();
        // time: 5, wait for peer be dropped
        time::sleep(Duration::from_millis(5)).await;
        assert_err!(ep1.split());

        let ep1 = ep1_fork.clone();
        // time: 15, wait for peer reopen
        time::sleep(Duration::from_millis(10)).await;
        assert_ok!(ep1.split());

        let ep1 = ep1_fork;
        // time: 25, wait for peer close
        time::sleep(Duration::from_millis(10)).await;
        assert_err!(ep1.split());
    });

    // time: 0
    drop(ep0);

    // time: 10, wait for the failed asseration
    time::sleep(Duration::from_millis(10)).await;
    let ep0_new = assert_ok!(ep1.get_peer());

    // time: 20, wait for the ok asseration
    time::sleep(Duration::from_millis(10)).await;
    drop(ep0_new);

    assert_ok!(handler.await);
}

#[test(tokio::test)]
async fn test_wire_recv_after_another_peer_reopend() {
    let (ep0, ep1) = Wire::<u32>::endpoints();

    let ep1_fork = ep1.clone();

    let handler = tokio::spawn(async move {
        let (_, mut rx) = assert_ok!(ep1_fork.split());
        // time: 10, wait for peer be dropped
        time::sleep(Duration::from_millis(10)).await;
        assert_err!(rx.recv().await);

        time::sleep(Duration::from_millis(1)).await;
        assert_err!(rx.recv().await);
    });

    // time: 5, wait for split
    time::sleep(Duration::from_millis(5)).await;
    drop(ep0);

    assert_ok!(handler.await);
}
