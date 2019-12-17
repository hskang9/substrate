// Copyright 2019 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

//! Block finalization utilities

use crate::rpc;
use sp_runtime::{
	Justification,
	traits::Block as BlockT,
	generic::BlockId,
};
use std::{
	sync::Arc,
	marker::PhantomData
};
use hash_db::Hasher;
use client_api::backend::Backend as ClientBackend;

pub struct FinalizeBlockParams<B: BlockT, H, CB> {
	pub hash: <B as BlockT>::Hash,
	pub sender: rpc::Sender<()>,
	pub justification: Option<Justification>,
	pub back_end: Arc<CB>,
	pub _phantom: PhantomData<H>
}

pub fn finalize_block<B, H, CB>(params: FinalizeBlockParams<B, H, CB>)
	where
		B: BlockT,
		H: Hasher<Out=<B as BlockT>::Hash>,
		CB: ClientBackend<B, H>,
{
	let FinalizeBlockParams {
		hash,
		sender,
		justification,
		back_end,
		..
	} = params;

	match back_end.finalize_block(BlockId::Hash(hash), justification) {
		Err(e) => {
			log::warn!("Failed to finalize block {:?}", e);
			rpc::send_result(sender, Err(e.into()))
		}
		Ok(()) => {
			log::info!("Successfully finalized block: {}", hash);
			rpc::send_result(sender, Ok(()))
		}
	}
}