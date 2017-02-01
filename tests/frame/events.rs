use cdrs::frame::events::ServerEvent;

#[test]
fn test_server_event_as_string() {
    assert_eq!(ServerEvent::TopologyChange.as_string(), "TOPOLOGY_CHANGE".to_string());
    assert_eq!(ServerEvent::StatusChange.as_string(), "STATUS_CHANGE".to_string());
    // TODO:
    // assert_eq!(ServerEvent::TopologyChange, "TOPOLOGY_CHANGE".to_string());
}
