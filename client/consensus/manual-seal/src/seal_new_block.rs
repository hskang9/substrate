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

//! Block sealing utilities

use crate::{Error, rpc};
use std::sync::Arc;
use sp_runtime::{
	traits::{Block as BlockT, Header as HeaderT},
	generic::BlockId,
};
use transaction_pool::txpool;
use rpc::CreatedBlock;

use consensus_common::{
	self, BlockImport, Environment, Proposer,
	ForkChoiceStrategy, BlockImportParams, BlockOrigin,
	ImportResult, SelectChain,
	import_queue::BoxBlockImport,
};
use sp_blockchain::HeaderBackend;
use client_api::backend::Backend as ClientBackend;
use hash_db::Hasher;
use std::collections::HashMap;
use std::time::Duration;
use std::marker::PhantomData;

pub struct SealBlockParams<'a, B: BlockT, C, CB, E, P: txpool::ChainApi, H> {
	pub create_empty: bool,
	pub finalize: bool,
	pub parent_hash: Option<<B as BlockT>::Hash>,
	pub sender: rpc::Sender<CreatedBlock<<B as BlockT>::Hash>>,
	pub pool: Arc<txpool::Pool<P>>,
	pub back_end: Arc<CB>,
	pub env: &'a mut E,
	pub select_chain: &'a C,
	pub block_import: &'a mut BoxBlockImport<B>,
	pub inherent_data_provider: &'a inherents::InherentDataProviders,
	pub _phantom: PhantomData<H>
}

pub async fn seal_new_block<B, C, CB, E, P, H>(params: SealBlockParams<'_, B, C, CB, E, P, H>)
	where
		B: BlockT,
		H: Hasher<Out=<B as BlockT>::Hash>,
		CB: ClientBackend<B, H>,
		E: Environment<B>,
		E::Error: std::fmt::Display,
		<E::Proposer as Proposer<B>>::Error: std::fmt::Display,
		P: txpool::ChainApi<Block=B, Hash=<B as BlockT>::Hash>,
		C: SelectChain<B>,
{
	let SealBlockParams {
		create_empty,
		finalize,
		pool,
		parent_hash,
		sender,
		back_end,
		select_chain,
		block_import,
		env,
		inherent_data_provider,
		..
	} = params;

	if pool.status().ready == 0 && !create_empty {
		return rpc::send_result(sender, Err(Error::EmptyTransactionPool));
	}

	// get the header to build this new block on.
	// use the parent_hash supplied via `EngineCommand`
	// or fetch the best_block.
	let header = match parent_hash {
		Some(hash) => {
			back_end.blockchain().header(BlockId::Hash(hash))
				.map_err(Error::from)
				.and_then(|header| {
					match header {
						Some(header) => Ok(header),
						None => Err(Error::BlockNotFound)
					}
				})
		}
		None => select_chain.best_chain().map_err(Error::from)
	};

	let header = match header {
		Err(e) => {
			return rpc::send_result(sender, Err(e));
		}
		Ok(h) => h,
	};

	let mut proposer = match env.init(&header) {
		Err(err) => {
			return rpc::send_result(sender, Err(Error::ProposerError(format!("{}", err))));
		}
		Ok(p) => p,
	};

	let id = match inherent_data_provider.create_inherent_data() {
		Err(err) => {
			return rpc::send_result(sender, Err(err.into()));
		}
		Ok(id) => id,
	};

	let result = proposer.propose(
		id,
		Default::default(),
		Duration::from_secs(5),
	).await;

	let (params, header) = match result {
		Ok(block) => {
			let (header, body) = block.deconstruct();
			(BlockImportParams {
				origin: BlockOrigin::Own,
				header: header.clone(),
				justification: None,
				post_digests: Vec::new(),
				body: Some(body),
				finalized: finalize,
				auxiliary: Vec::new(),
				fork_choice: ForkChoiceStrategy::LongestChain,
				allow_missing_state: false,
				import_existing: false,
			}, header)
		}
		Err(err) => {
			return rpc::send_result(sender, Err(Error::ProposerError(format!("{}", err))));
		}
	};

	match block_import.import_block(params, HashMap::new()) {
		Ok(ImportResult::Imported(aux)) => {
			rpc::send_result(sender, Ok(CreatedBlock { hash: <B as BlockT>::Header::hash(&header), aux }))
		}
		Ok(other) => rpc::send_result(sender, Err(other.into())),
		Err(e) => {
			log::warn!("Failed to import block: {:?}", e);
			rpc::send_result(sender, Err(e.into()))
		}
	};
}
