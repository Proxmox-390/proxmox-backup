use anyhow::Error;
use serde_json::Value;

use proxmox_config_digest::ConfigDigest;
use proxmox_notify::api_types::{
    DeletableGotifyProperty, ENTITY_NAME_SCHEMA, GotifyConfig, GotifyConfigUpdater,
    GotifyPrivateConfig, GotifyPrivateConfigUpdater,
};
use proxmox_router::{Permission, Router, RpcEnvironment};
use proxmox_schema::api;

use pbs_api_types::{PRIV_SYS_AUDIT, PRIV_SYS_MODIFY, PROXMOX_CONFIG_DIGEST_SCHEMA};

use crate::server;

#[api(
    protected: true,
    input: {
        properties: {},
    },
    returns: {
        description: "List of gotify endpoints.",
        type: Array,
        items: { type: GotifyConfig },
    },
    access: {
        permission: &Permission::Privilege(&["system", "notifications"], PRIV_SYS_AUDIT, false),
    },
)]
/// List all gotify endpoints.
pub fn list_endpoints(
    _param: Value,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<GotifyConfig>, Error> {
    let config = pbs_config::notifications::config(server::notifications::context())?;

    let endpoints = proxmox_notify::api::gotify::get_endpoints(&config)?;

    Ok(endpoints)
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: ENTITY_NAME_SCHEMA,
            }
        },
    },
    returns: { type: GotifyConfig },
    access: {
        permission: &Permission::Privilege(&["system", "notifications"], PRIV_SYS_AUDIT, false),
    },
)]
/// Get a gotify endpoint.
pub fn get_endpoint(name: String, rpcenv: &mut dyn RpcEnvironment) -> Result<GotifyConfig, Error> {
    let config = pbs_config::notifications::config(server::notifications::context())?;
    let endpoint = proxmox_notify::api::gotify::get_endpoint(&config, &name)?;

    rpcenv["digest"] = config.digest().to_hex().into();

    Ok(endpoint)
}

#[api(
    protected: true,
    input: {
        properties: {
            endpoint: {
                type: GotifyConfig,
                flatten: true,
            },
            token: {
                description: "Authentication token",
            }
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "notifications"], PRIV_SYS_MODIFY, false),
    },
)]
/// Add a new gotify endpoint.
pub fn add_endpoint(
    endpoint: GotifyConfig,
    token: String,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let _lock = pbs_config::notifications::lock_config()?;
    let mut config = pbs_config::notifications::config(server::notifications::context())?;
    let private_endpoint_config = GotifyPrivateConfig {
        name: endpoint.name.clone(),
        token,
    };

    proxmox_notify::api::gotify::add_endpoint(&mut config, endpoint, private_endpoint_config)?;

    pbs_config::notifications::save_config(config)?;
    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: ENTITY_NAME_SCHEMA,
            },
            updater: {
                type: GotifyConfigUpdater,
                flatten: true,
            },
            token: {
                description: "Authentication token",
                optional: true,
            },
            delete: {
                description: "List of properties to delete.",
                type: Array,
                optional: true,
                items: {
                    type: DeletableGotifyProperty,
                }
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "notifications"], PRIV_SYS_MODIFY, false),
    },
)]
/// Update gotify endpoint.
pub fn update_endpoint(
    name: String,
    updater: GotifyConfigUpdater,
    token: Option<String>,
    delete: Option<Vec<DeletableGotifyProperty>>,
    digest: Option<ConfigDigest>,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let _lock = pbs_config::notifications::lock_config()?;
    let mut config = pbs_config::notifications::config(server::notifications::context())?;

    proxmox_notify::api::gotify::update_endpoint(
        &mut config,
        &name,
        updater,
        GotifyPrivateConfigUpdater { token },
        delete.as_deref(),
        digest,
    )?;

    pbs_config::notifications::save_config(config)?;
    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: ENTITY_NAME_SCHEMA,
            }
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "notifications"], PRIV_SYS_MODIFY, false),
    },
)]
/// Delete gotify endpoint.
pub fn delete_endpoint(name: String, _rpcenv: &mut dyn RpcEnvironment) -> Result<(), Error> {
    let _lock = pbs_config::notifications::lock_config()?;
    let mut config = pbs_config::notifications::config(server::notifications::context())?;
    proxmox_notify::api::gotify::delete_gotify_endpoint(&mut config, &name)?;

    pbs_config::notifications::save_config(config)?;
    Ok(())
}

const ITEM_ROUTER: Router = Router::new()
    .get(&API_METHOD_GET_ENDPOINT)
    .put(&API_METHOD_UPDATE_ENDPOINT)
    .delete(&API_METHOD_DELETE_ENDPOINT);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_ENDPOINTS)
    .post(&API_METHOD_ADD_ENDPOINT)
    .match_all("name", &ITEM_ROUTER);
