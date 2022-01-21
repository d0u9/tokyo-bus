// This test file is references directly via
// #[path = "unit_tests/packet_endpoint_test.rs"]
// from packet_endpoint.rs
//
// use std::mem::drop;

use test_log::test;
use claim::assert_err;
use claim::assert_ok;
use tokio::time::{self, Duration};

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

        assert_err!(rx_ep1.recv().await);
    });

    time::sleep(Duration::from_millis(3)).await;
    assert_ok!(tx_ep0.send_data(&this_addr, test_val));

    let another_addr = Address::random();
    assert_ok!(tx_ep0.send_data(&another_addr, test_val));

    assert_ok!(handler.await);
}
