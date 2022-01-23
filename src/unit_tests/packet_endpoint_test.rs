// This test file is references directly via
// #[path = "unit_tests/packet_endpoint_test.rs"]
// from packet_endpoint.rs
//
// use std::mem::drop;

use claim::assert_ok;
use test_log::test;
use tokio::time::{self, Duration};
use tokio_stream::StreamExt;

use super::*;

#[test(tokio::test)]
async fn test_packet_endpoint_functional() {
    let this_addr = Address::new("this");
    let (ep0, ep1) = PktWire::<u32>::endpoints(this_addr.clone());
    let (tx_ep0, _) = assert_ok!(ep0.split());
    let (_, mut rx_ep1) = assert_ok!(ep1.split());

    let test_val = 0xdeadbeef_u32;

    let handler = tokio::spawn(async move {
        let pkt = assert_ok!(rx_ep1.recv().await);
        assert_eq!(pkt.into_inner(), test_val);

        let pkt = assert_ok!(rx_ep1.recv().await);
        assert_eq!(pkt.into_inner(), test_val);
    });

    time::sleep(Duration::from_millis(3)).await;
    assert_ok!(tx_ep0.send_data(&this_addr, test_val));

    let another_addr = Address::random();
    assert_ok!(tx_ep0.send_data(&another_addr, test_val));

    assert_ok!(handler.await);
}

#[test(tokio::test)]
async fn test_packet_stream_poll() {
    let this_addr = Address::new("this");
    let (ep0, ep1) = PktWire::<u32>::endpoints(this_addr.clone());
    let (tx_ep0, _) = assert_ok!(ep0.split());

    let test_val = 0xdeadbeef_u32;

    let handler = tokio::spawn(async move {
        let (_, rx_ep1) = assert_ok!(ep1.split());
        let mut rx_stream = PktRxStream::new(rx_ep1);

        while let Some(v) = rx_stream.next().await {
            println!("received {:?}", v);
        }
    });

    time::sleep(Duration::from_millis(3)).await;
    assert_ok!(tx_ep0.send_data(&this_addr, test_val));

    drop(tx_ep0);

    assert_ok!(handler.await);
}
