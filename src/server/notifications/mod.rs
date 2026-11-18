use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Error, bail, format_err};
use const_format::concatcp;
use nix::sys::stat::Mode;
use nix::unistd::{Gid, Uid};

use proxmox_notify::context::pbs::PBSContext;
use proxmox_notify::queue::{QueueConsumer, QueueSubmitter, QueueWorker};
use proxmox_notify::{Context, EmailRecipient, Notification, Recipient, Severity};
use proxmox_schema::ApiType;
use proxmox_sys::fs::{CreateOptions, create_path};
use proxmox_uuid::Uuid;

use crate::tape::TapeNotificationMode;
use pbs_api_types::{
    APTUpdateInfo, DataStoreConfig, DatastoreNotify, GarbageCollectionStatus, NotificationMode,
    Notify, SyncJobConfig, TapeBackupJobSetup, User, Userid, VerificationJobConfig,
};

const NOTIFICATION_BASE_DIR: &str =
    concatcp!(pbs_buildcfg::PROXMOX_BACKUP_STATE_DIR, "/notifications");
const QUEUE_DIR: &str = concatcp!(NOTIFICATION_BASE_DIR, "/queue");

mod template_data;

use template_data::{
    AcmeErrTemplateData, CommonData, DatastoreThresholdExceededTemplateData, GcErrTemplateData,
    GcOkTemplateData, PackageUpdatesTemplateData, PruneErrTemplateData, PruneOkTemplateData,
    SyncErrTemplateData, SyncOkTemplateData, TapeBackupErrTemplateData, TapeBackupOkTemplateData,
    TapeLoadTemplateData, VerifyErrTemplateData, VerifyOkTemplateData,
};

static CONTEXT: OnceLock<Context> = OnceLock::new();

/// Get a reference to the static notification context for PBS.
pub fn context() -> &'static Context {
    CONTEXT.get_or_init(|| Context::new(PBSContext))
}

/// Creates directories used for queuing notifications.
pub fn create_notification_dirs() -> Result<(), Error> {
    let backup_user = proxmox_product_config::get_api_user();
    let opts = CreateOptions::new()
        .owner(backup_user.uid)
        .group(backup_user.gid);

    create_path(NOTIFICATION_BASE_DIR, None, Some(opts))?;

    ensure_queue_dir(proxmox_product_config::get_priv_user().uid, backup_user.gid)
}

/// Make sure that the queue directory exists, is owned by `uid` and `gid` and has mode 3770.
///
/// The queue is writable by anybody in the 'backup' group, the sticky bit keeps them from removing
/// or replacing each other's notifications. As the parent directory belongs to the backup user,
/// the queue directory is opened without following symlinks and fixed through that handle.
fn ensure_queue_dir(uid: Uid, gid: Gid) -> Result<(), Error> {
    const QUEUE_DIR_MODE: u32 = 0o3770;

    let created = match nix::unistd::mkdir(QUEUE_DIR, Mode::from_bits_truncate(0o700)) {
        Ok(()) => true,
        Err(nix::errno::Errno::EEXIST) => false,
        Err(err) => bail!("unable to create {QUEUE_DIR} - {err}"),
    };

    let dir = std::fs::File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(QUEUE_DIR)
        .map_err(|err| format_err!("unable to open {QUEUE_DIR} - {err}"))?;
    let metadata = dir.metadata()?;

    let wrong_owner = metadata.uid() != uid.as_raw() || metadata.gid() != gid.as_raw();
    let wrong_mode = metadata.mode() & 0o7777 != QUEUE_DIR_MODE;

    if !created && (wrong_owner || wrong_mode) {
        log::warn!("fixing unexpected owner or mode of {QUEUE_DIR}");
    }

    if wrong_owner {
        nix::unistd::fchown(dir.as_raw_fd(), Some(uid), Some(gid))
            .map_err(|err| format_err!("unable to set owner of {QUEUE_DIR} - {err}"))?;
    }

    // Set the mode after the owner, as changing the owner can clear the setgid bit.
    if wrong_owner || wrong_mode {
        nix::sys::stat::fchmod(dir.as_raw_fd(), Mode::from_bits_truncate(QUEUE_DIR_MODE))
            .map_err(|err| format_err!("unable to set mode of {QUEUE_DIR} - {err}"))?;
    }

    Ok(())
}

/// Worker task to periodically send any queued notifications.
pub async fn notification_worker() {
    let consumer = QueueConsumer::new(QUEUE_DIR);

    let worker = QueueWorker::builder(context().clone(), consumer)
        .interval(Duration::from_secs(5))
        .retry_attempts(2)
        .build();

    tokio::select! {
        _ = worker.run() => {}
        _ = migrate_leftover_notifications_loop() => {}
    }
}

/// Periodically move notifications from the old spool directory to the new queue.
///
/// After an upgrade, proxy processes running the old binary keep spooling notifications there
/// until all of their worker tasks have finished, so checking only at startup is not enough.
///
/// FIXME: MAJOR VERSION: Remove
async fn migrate_leftover_notifications_loop() {
    loop {
        match tokio::task::spawn_blocking(migrate_leftover_notifications).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => log::error!("failed to migrate notifications: {err:#}"),
            Err(err) => log::error!("notification migration task failed: {err}"),
        }

        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// Move any left-over notifications from the old spool directory at
/// /var/lib/proxmox-backup/notifications to the new queue
/// at /var/lib/proxmox-backup/notifications/queue
fn migrate_leftover_notifications() -> Result<(), Error> {
    let read_dir = std::fs::read_dir(NOTIFICATION_BASE_DIR)?;

    for entry in read_dir.flatten() {
        if let Some((name, ext)) = entry.file_name().to_string_lossy().split_once(".") {
            if ext == "json" && Uuid::parse_str(name).is_ok() {
                let path = entry.path();

                if let Err(err) = migrate_single_notification(&path) {
                    log::error!(
                        "failed to migrate notification at '{p}' to new queue: {err:#}",
                        p = path.display()
                    );
                }
            }
        }
    }

    Ok(())
}

fn migrate_single_notification(path: &Path) -> Result<(), Error> {
    let bytes = std::fs::read(path)?;

    // Technically, we don't need to parse, but this is just to ensure that
    // we don't migrate over something that is not understood by the
    // notification worker.
    let notification: Notification = serde_json::from_slice(&bytes)?;

    let uuid = notification.id().clone();

    // Manually move the notification into the new queue directory.
    //
    // Use a manual rename instead of resubmitting to ensure atomicity.
    std::fs::rename(
        path,
        Path::new(QUEUE_DIR).join(format!("notification-{uuid}")),
    )?;

    log::info!("moved notification {uuid} to new notification queue");

    Ok(())
}

fn send_notification(notification: Notification) -> Result<(), Error> {
    let writer = QueueSubmitter::new(QUEUE_DIR);

    writer.submit(&notification)?;
    Ok(())
}

/// Summary of a successful Tape Job
#[derive(Default)]
pub struct TapeBackupJobSummary {
    /// The list of snaphots backed up
    pub snapshot_list: Vec<String>,
    /// The total time of the backup job
    pub duration: std::time::Duration,
    /// The labels of the used tapes of the backup job
    pub used_tapes: Option<Vec<String>>,
}

pub fn send_gc_status(
    datastore: &str,
    status: &GarbageCollectionStatus,
    result: &Result<(), Error>,
) -> Result<(), Error> {
    let Some(recipient) =
        datastore_notification_recipient(datastore, |n| n.gc, Notify::Always, result.is_ok())
    else {
        return Ok(());
    };

    let builder = match result {
        Ok(()) => {
            let template_data = GcOkTemplateData::new(datastore.to_string(), status);
            Notification::template_builder("gc-ok", serde_json::to_value(template_data)?)
                .severity(Severity::Info)
        }
        Err(err) => {
            let template_data = GcErrTemplateData::new(datastore.to_string(), format!("{err:#}"));
            Notification::template_builder("gc-err", serde_json::to_value(template_data)?)
                .severity(Severity::Error)
        }
    };

    let notification = builder
        .field("type", "gc")
        .field("datastore", datastore)
        .field("hostname", proxmox_sys::nodename())
        .recipient(recipient)
        .build()?;

    send_notification(notification)?;

    Ok(())
}

pub fn send_verify_status(
    job: VerificationJobConfig,
    result: &Result<Vec<String>, Error>,
) -> Result<(), Error> {
    // Failed snapshots are reported via `Ok`, but still count as an error for the notify setting.
    let is_ok = matches!(result, Ok(errors) if errors.is_empty());

    let Some(recipient) =
        datastore_notification_recipient(&job.store, |n| n.verify, Notify::Always, is_ok)
    else {
        return Ok(());
    };

    let builder = match result {
        Err(_) => {
            // aborted job - do not send any notification
            return Ok(());
        }
        Ok(errors) if errors.is_empty() => {
            let template_data = VerifyOkTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: job.id.clone(),
            };
            Notification::template_builder("verify-ok", serde_json::to_value(template_data)?)
                .severity(Severity::Info)
        }
        Ok(errors) => {
            let template_data = VerifyErrTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: job.id.clone(),
                failed_snapshot_list: errors.clone(),
            };
            Notification::template_builder("verify-err", serde_json::to_value(template_data)?)
                .severity(Severity::Error)
        }
    };

    let notification = builder
        .field("type", "verify")
        .field("job-id", job.id)
        .field("datastore", job.store)
        .field("hostname", proxmox_sys::nodename())
        .recipient(recipient)
        .build()?;

    send_notification(notification)?;

    Ok(())
}

pub fn send_prune_status(
    store: &str,
    jobname: &str,
    result: &Result<(), Error>,
) -> Result<(), Error> {
    let Some(recipient) =
        datastore_notification_recipient(store, |n| n.prune, Notify::Error, result.is_ok())
    else {
        return Ok(());
    };

    let builder = match result {
        Ok(()) => {
            let template_data = PruneOkTemplateData {
                common: CommonData::new(),
                datastore: store.to_string(),
                job_id: jobname.to_string(),
            };

            Notification::template_builder("prune-ok", serde_json::to_value(template_data)?)
                .severity(Severity::Info)
        }
        Err(err) => {
            let template_data = PruneErrTemplateData {
                common: CommonData::new(),
                datastore: store.to_string(),
                job_id: jobname.to_string(),
                error: format!("{err:#}"),
            };

            Notification::template_builder("prune-err", serde_json::to_value(template_data)?)
                .severity(Severity::Error)
        }
    };

    let notification = builder
        .field("type", "prune")
        .field("job-id", jobname)
        .field("datastore", store)
        .field("hostname", proxmox_sys::nodename())
        .recipient(recipient)
        .build()?;

    send_notification(notification)?;

    Ok(())
}

pub fn send_sync_status(job: &SyncJobConfig, result: &Result<(), Error>) -> Result<(), Error> {
    let Some(recipient) =
        datastore_notification_recipient(&job.store, |n| n.sync, Notify::Always, result.is_ok())
    else {
        return Ok(());
    };

    let builder = match result {
        Ok(()) => {
            let template_data = SyncOkTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: job.id.clone(),
                remote: job.remote.clone(),
                remote_datastore: job.remote_store.clone(),
            };
            Notification::template_builder("sync-ok", serde_json::to_value(template_data)?)
                .severity(Severity::Info)
        }
        Err(err) => {
            let template_data = SyncErrTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: job.id.clone(),
                remote: job.remote.clone(),
                remote_datastore: job.remote_store.clone(),
                error: format!("{err:#}"),
            };
            Notification::template_builder("sync-err", serde_json::to_value(template_data)?)
                .severity(Severity::Error)
        }
    };

    let notification = builder
        .field("type", "sync")
        .field("job-id", &job.id)
        .field("datastore", &job.store)
        .field("hostname", proxmox_sys::nodename())
        .recipient(recipient)
        .build()?;

    send_notification(notification)?;

    Ok(())
}

pub fn send_tape_backup_status(
    id: Option<&str>,
    job: &TapeBackupJobSetup,
    result: &Result<(), Error>,
    summary: TapeBackupJobSummary,
) -> Result<(), Error> {
    let Some(recipient) = tape_notification_recipient(&TapeNotificationMode::from(job)) else {
        return Ok(());
    };

    let duration = summary.duration.as_secs();

    let mut builder = match result {
        Ok(()) => {
            let template_data = TapeBackupOkTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: id.map(|id| id.into()),
                job_duration: duration,
                tape_pool: job.pool.clone(),
                tape_drive: job.drive.clone(),
                used_tapes_list: summary.used_tapes.unwrap_or_default(),
                snapshot_list: summary.snapshot_list,
            };

            Notification::template_builder("tape-backup-ok", serde_json::to_value(template_data)?)
                .severity(Severity::Info)
        }
        Err(err) => {
            let template_data = TapeBackupErrTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: id.map(|id| id.into()),
                job_duration: duration,
                tape_pool: job.pool.clone(),
                tape_drive: job.drive.clone(),
                used_tapes_list: summary.used_tapes.unwrap_or_default(),
                snapshot_list: summary.snapshot_list,
                error: format!("{err:#}"),
            };

            Notification::template_builder("tape-backup-err", serde_json::to_value(template_data)?)
                .severity(Severity::Error)
        }
    };

    if let Some(id) = id {
        builder = builder.field("job-id", id);
    }

    let notification = builder
        .field("type", "tape-backup")
        .field("datastore", &job.store)
        .field("media-pool", &job.pool)
        .field("hostname", proxmox_sys::nodename())
        .recipient(recipient)
        .build()?;

    send_notification(notification)?;

    Ok(())
}

/// Send email to a person to request a manual media change
pub fn send_load_media_notification(
    mode: &TapeNotificationMode,
    changer: bool,
    device: &str,
    label_text: &str,
    reason: Option<String>,
) -> Result<(), Error> {
    let Some(recipient) = tape_notification_recipient(mode) else {
        return Ok(());
    };

    let device_type = if changer { "changer" } else { "drive" };

    let template_data = TapeLoadTemplateData {
        common: CommonData::new(),
        load_reason: reason,
        tape_drive: device.into(),
        drive_type: device_type.into(),
        drive_is_changer: changer,
        tape_label: label_text.into(),
    };

    let notification =
        Notification::template_builder("tape-load", serde_json::to_value(template_data)?)
            .severity(Severity::Notice)
            .field("type", "tape-load")
            .field("hostname", proxmox_sys::nodename())
            .recipient(recipient)
            .build()?;

    send_notification(notification)?;

    Ok(())
}

pub fn send_updates_available(updates: &[&APTUpdateInfo]) -> Result<(), Error> {
    let template_data = PackageUpdatesTemplateData::new(updates);

    let notification =
        Notification::template_builder("package-updates", serde_json::to_value(template_data)?)
            .severity(Severity::Info)
            .field("type", "package-updates")
            .field("hostname", proxmox_sys::nodename())
            .build()?;

    send_notification(notification)?;
    Ok(())
}

/// send email on certificate renewal failure.
pub fn send_certificate_renewal_mail(result: &Result<(), Error>) -> Result<(), Error> {
    let error: String = match result {
        Err(e) => format!("{e:#}"),
        _ => return Ok(()),
    };

    let template_data = AcmeErrTemplateData {
        common: CommonData::new(),
        error,
    };

    let notification =
        Notification::template_builder("acme-err", serde_json::to_value(template_data)?)
            .severity(Severity::Error)
            .field("type", "acme")
            .field("hostname", proxmox_sys::nodename())
            .build()?;

    send_notification(notification)?;
    Ok(())
}

/// Send notification if datastore values are exceeding the set threshold limit.
pub fn send_datastore_threshold_exceeded(
    datastore: &str,
    threshold: &str,
    limit: u64,
    value: u64,
) -> Result<(), Error> {
    let is_traffic = threshold == "uploaded" || threshold == "downloaded";
    let template_data = DatastoreThresholdExceededTemplateData::new(
        datastore.to_string(),
        threshold.to_string(),
        limit,
        value,
        is_traffic,
    );

    let notification =
        Notification::template_builder("thresholds-exceeded", serde_json::to_value(template_data)?)
            .severity(Severity::Warning)
            .field("type", "thresholds")
            .field("datastore", datastore)
            .field("hostname", proxmox_sys::nodename())
            .build()?;

    send_notification(notification)?;
    Ok(())
}

/// Lookup users email address
pub fn lookup_user_email(userid: &Userid) -> Option<String> {
    if let Ok(user_config) = pbs_config::user::cached_config() {
        if let Ok(user) = user_config.lookup::<User>("user", userid.as_str()) {
            return user.email;
        }
    }

    None
}

/// Determine the recipient for a datastore-related notification.
///
/// Returns `None` if no notification should be sent, either because the legacy notify
/// settings of the datastore suppress it or because the notify user has no email address.
fn datastore_notification_recipient(
    store: &str,
    notify_setting: fn(&DatastoreNotify) -> Option<Notify>,
    default: Notify,
    is_ok: bool,
) -> Option<Recipient> {
    let (email, notify, mode) = lookup_datastore_notify_settings(store);

    match mode {
        NotificationMode::LegacySendmail => {
            let notify = notify_setting(&notify).unwrap_or(default);

            if notify == Notify::Never || (is_ok && notify == Notify::Error) {
                return None;
            }

            email.map(|email| {
                Recipient::Email(EmailRecipient {
                    mailto: vec![email],
                    ..Default::default()
                })
            })
        }
        NotificationMode::NotificationSystem => Some(Recipient::Matcher),
    }
}

/// Determine the recipient for a tape-related notification.
///
/// Returns `None` if no notification should be sent because the notify user has no email
/// address.
fn tape_notification_recipient(mode: &TapeNotificationMode) -> Option<Recipient> {
    match mode {
        TapeNotificationMode::LegacySendmail { notify_user } => {
            lookup_user_email(notify_user).map(|email| {
                Recipient::Email(EmailRecipient {
                    mailto: vec![email],
                    ..Default::default()
                })
            })
        }
        TapeNotificationMode::NotificationSystem => Some(Recipient::Matcher),
    }
}

/// Lookup Datastore notify settings
pub fn lookup_datastore_notify_settings(
    store: &str,
) -> (Option<String>, DatastoreNotify, NotificationMode) {
    let mut email = None;

    let notify = DatastoreNotify {
        gc: None,
        verify: None,
        sync: None,
        prune: None,
    };

    let (config, _digest) = match pbs_config::datastore::config() {
        Ok(result) => result,
        Err(_) => return (email, notify, NotificationMode::default()),
    };

    let config: DataStoreConfig = match config.lookup("datastore", store) {
        Ok(result) => result,
        Err(_) => return (email, notify, NotificationMode::default()),
    };

    email = match config.notify_user {
        Some(ref userid) => lookup_user_email(userid),
        None => lookup_user_email(Userid::root_userid()),
    };

    let notification_mode = config.notification_mode.unwrap_or_default();
    let notify_str = config.notify.unwrap_or_default();

    if let Ok(value) = DatastoreNotify::API_SCHEMA.parse_property_string(&notify_str) {
        if let Ok(notify) = serde_json::from_value(value) {
            return (email, notify, notification_mode);
        }
    }

    (email, notify, notification_mode)
}
