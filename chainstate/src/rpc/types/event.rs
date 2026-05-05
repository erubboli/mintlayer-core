// Copyright (c) 2023 RBB S.r.l
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

use common::{
    chain::GenBlock,
    primitives::{BlockHeight, Id},
};

use crate::ChainstateEvent;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, rpc::description::HasValueHint)]
#[serde(tag = "type", content = "content")]
pub enum RpcEvent {
    NewTip {
        id: Id<GenBlock>,
        height: BlockHeight,
    },
    Reorganized {
        old_tip_id: Id<GenBlock>,
        old_tip_height: BlockHeight,
        new_tip_id: Id<GenBlock>,
        new_tip_height: BlockHeight,
    },
}

impl RpcEvent {
    pub fn from_event(event: ChainstateEvent) -> Self {
        match event {
            ChainstateEvent::NewTip {
                id,
                height,
                is_initial_block_download: _,
            } => Self::NewTip { id, height },
            ChainstateEvent::Reorganized {
                old_tip_id,
                old_tip_height,
                new_tip_id,
                new_tip_height,
            } => Self::Reorganized {
                old_tip_id,
                old_tip_height,
                new_tip_id,
                new_tip_height,
            },
        }
    }
}
