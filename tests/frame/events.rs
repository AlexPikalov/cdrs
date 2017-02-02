use cdrs::frame::events::SimpleServerEvent;

#[test]
fn test_server_event_as_string() {
    assert_eq!(SimpleServerEvent::TopologyChange.as_string(), "TOPOLOGY_CHANGE".to_string());
    assert_eq!(SimpleServerEvent::StatusChange.as_string(), "STATUS_CHANGE".to_string());
    // TODO:
    // assert_eq!(ServerEvent::TopologyChange, "TOPOLOGY_CHANGE".to_string());
}
