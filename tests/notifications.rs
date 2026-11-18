use proxmox_notify::Notification;

/// Ensures that a notification serialized before switching to the shared notification worker
/// continues to parse as the `Notification` type from proxmox-notify.
#[test]
fn test_parse_serialized_notification() {
    let data = std::fs::read("tests/data/serialized_notification.json").unwrap();
    let notification: Notification = serde_json::from_slice(&data).unwrap();

    assert_eq!(notification.timestamp(), 1792761135);
}
