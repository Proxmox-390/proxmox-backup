use anyhow::{Context, Error, bail};
use serde_json::{Value, json};

use proxmox_notify::{
    Notification, Recipient, Severity,
    api_types::{ENTITY_NAME_SCHEMA, Target, TargetReceiptStatus},
};
use proxmox_router::{Permission, Router, RpcEnvironment, SubdirMap, list_subdirs_api_method};
use proxmox_schema::api;
use proxmox_sortable_macro::sortable;

use pbs_api_types::{PRIV_SYS_AUDIT, PRIV_SYS_MODIFY};

use crate::server;

#[api(
    protected: true,
    input: {
        properties: {},
    },
    returns: {
        description: "List of all entities which can be used as notification targets.",
        type: Array,
        items: { type: Target },
    },
    access: {
        permission: &Permission::Privilege(&["system", "notifications"], PRIV_SYS_AUDIT, false),
    },
)]
/// List all notification targets.
pub fn list_targets(_param: Value, _rpcenv: &mut dyn RpcEnvironment) -> Result<Vec<Target>, Error> {
    let config = pbs_config::notifications::config(server::notifications::context())?;
    let targets = proxmox_notify::api::get_targets(&config)?;

    Ok(targets)
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: ENTITY_NAME_SCHEMA,
            },
        }
    },
    access: {
        permission: &Permission::Privilege(&["system", "notifications"], PRIV_SYS_MODIFY, false),
    },
)]
/// Test a given notification target.
pub fn test_target(name: String, _rpcenv: &mut dyn RpcEnvironment) -> Result<(), Error> {
    let context = server::notifications::context();
    let mut config = pbs_config::notifications::config(context)?;

    // Testing disabled targets must work too; the modified config is not persisted.
    proxmox_notify::api::set_entity_disabled(&mut config, &name, false)?;

    let notification = Notification::template_builder("test", json!({ "target": &name }))
        .severity(Severity::Info)
        .recipient(Recipient::Target(name.clone()))
        .build()?;

    let receipt = proxmox_notify::send(context, &config, &notification)?;

    match receipt
        .target_receipts()
        .first()
        .context("target receipt array did not contain expected entry")?
        .status()
    {
        TargetReceiptStatus::Delivered => Ok(()),
        TargetReceiptStatus::Error { error_message } => {
            bail!("could not test target: {error_message}")
        }
        TargetReceiptStatus::Disabled => {
            bail!(
                "target {name} disabled while sending test notification -- this should not happen"
            )
        }
    }
}

#[sortable]
const SUBDIRS: SubdirMap = &sorted!([("test", &TEST_ROUTER),]);
const TEST_ROUTER: Router = Router::new().post(&API_METHOD_TEST_TARGET);
const ITEM_ROUTER: Router = Router::new()
    .get(&list_subdirs_api_method!(SUBDIRS))
    .subdirs(SUBDIRS);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_TARGETS)
    .match_all("name", &ITEM_ROUTER);
