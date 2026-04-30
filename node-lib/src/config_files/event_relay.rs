// Copyright (c) 2024 RBB S.r.l
// opensource@mintlayer.org
// SPDX-License-Identifier: MIT
// Licensed under the MIT License;
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://github.com/mintlayer/mintlayer-core/blob/master/LICENSE
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{net::SocketAddr, str::FromStr};

use chainstate_launcher::ChainConfig;
use serde::{Deserialize, Serialize};

use crate::RunOptions;

/// Configuration for the WebSocket event relay subsystem.
#[must_use]
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct EventRelayConfigFile {
    /// Whether the WebSocket event relay is enabled.
    pub event_relay_enabled: Option<bool>,

    /// Address to bind the WebSocket server to.
    pub bind_address: Option<SocketAddr>,
}

impl EventRelayConfigFile {
    pub fn default_bind_address(chain_config: &ChainConfig) -> SocketAddr {
        SocketAddr::from_str(&format!(
            "0.0.0.0:{}",
            chain_config.default_rpc_port() + 1
        ))
        .expect("Can't fail")
    }

    pub fn with_run_options(
        chain_config: &ChainConfig,
        config_file: EventRelayConfigFile,
        options: &RunOptions,
    ) -> EventRelayConfigFile {
        let EventRelayConfigFile {
            event_relay_enabled,
            bind_address,
        } = config_file;

        let event_relay_enabled = options
            .event_relay_enabled
            .unwrap_or_else(|| event_relay_enabled.unwrap_or(false));

        let bind_address = options.event_relay_bind_address.or(bind_address).unwrap_or_else(|| {
            Self::default_bind_address(chain_config)
        });

        EventRelayConfigFile {
            event_relay_enabled: Some(event_relay_enabled),
            bind_address: Some(bind_address),
        }
    }
}
