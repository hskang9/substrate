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

//! A manual sealing engine: the engine listens for rpc calls to seal blocks and create forks
//! This is suitable for a testing environment.

use consensus_common::{
	self, BlockImport, Environment, Proposer, BlockCheckParams,
	ForkChoiceStrategy, BlockImportParams, BlockOrigin,
	ImportResult, SelectChain,
	import_queue::{
		BasicQueue,
		CacheKeyId,
		Verifier,
		BoxBlockImport,
	},
};
use sp_runtime::{
	traits::{Block as BlockT, Header as HeaderT},
	generic::BlockId,
	Justification,
};
use sp_blockchain::HeaderBackend;
use client_api::backend::Backend as ClientBackend;
use futures::prelude::*;
use hash_db::Hasher;
use transaction_pool::{BasicPool, txpool};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub mod rpc;
mod error;

pub use error::Error;
pub use rpc::{EngineCommand, CreatedBlock};

/// The synchronous block-import worker of the engine.
pub struct ManualSealBlockImport<I> {
	inner: I,
}

impl<I> From<I> for ManualSealBlockImport<I> {
	fn from(i: I) -> Self {
		ManualSealBlockImport { inner: i }
	}
}

impl<B: BlockT, I: BlockImport<B>> BlockImport<B> for ManualSealBlockImport<I> {
	type Error = I::Error;

	fn check_block(&mut self, block: BlockCheckParams<B>) -> Result<ImportResult, Self::Error>
	{
		self.inner.check_block(block)
	}

	fn import_block(
		&mut self,
		block: BlockImportParams<B>,
		cache: HashMap<CacheKeyId, Vec<u8>>,
	) -> Result<ImportResult, Self::Error> {
		// TODO: strip out post-digest.

		self.inner.import_block(block, cache)
	}
}

/// The verifier for the manual seal engine; instantly finalizes.
struct ManualSealVerifier;

impl<B: BlockT> Verifier<B> for ManualSealVerifier {
	fn verify(
		&mut self,
		origin: BlockOrigin,
		header: B::Header,
		justification: Option<Justification>,
		body: Option<Vec<B::Extrinsic>>,
	) -> Result<(BlockImportParams<B>, Option<Vec<(CacheKeyId, Vec<u8>)>>), String> {
		let import_params = BlockImportParams {
			origin,
			header,
			justification,
			post_digests: Vec::new(),
			body,
			finalized: true,
			auxiliary: Vec::new(),
			fork_choice: ForkChoiceStrategy::LongestChain,
			allow_missing_state: false,
			import_existing: false,
		};

		Ok((import_params, None))
	}
}

/// Instantiate the import queue for the manual seal consensus engine.
pub fn import_queue<B: BlockT>(block_import: BoxBlockImport<B>) -> BasicQueue<B>
{
	BasicQueue::new(
		ManualSealVerifier,
		block_import,
		None,
		None,
	)
}

/// Creates the background authorship task for the manual seal engine.
pub async fn run_manual_seal<B, CB, E, A, C, S, H>(
	mut block_import: BoxBlockImport<B>,
	mut env: E,
	back_end: Arc<CB>,
	basic_pool: Arc<BasicPool<A, B>>,
	mut seal_block_channel: S,
	select_chain: C,
	inherent_data_providers: inherents::InherentDataProviders,
)
	where
		B: BlockT + 'static,
		H: Hasher<Out=<B as BlockT>::Hash>,
		CB: ClientBackend<B, H> + 'static,
		E: Environment<B> + 'static,
		E::Error: std::fmt::Display,
		<E::Proposer as Proposer<B>>::Error: std::fmt::Display,
		A: txpool::ChainApi<Block=B, Hash=<B as BlockT>::Hash> + 'static,
		S: Stream<Item=EngineCommand<<B as BlockT>::Hash>> + Unpin + 'static,
		C: SelectChain<B> + 'static,
{
	let pool = basic_pool.pool();

	while let Some(command) = seal_block_channel.next().await {
		match command {
			EngineCommand::SealNewBlock {
				create_empty,
				parent_hash,
				sender,
			} => {
				if pool.status().ready == 0 && !create_empty {
					rpc::send_result(sender, Err(Error::EmptyTransactionPool));
					continue
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
						rpc::send_result(sender, Err(e));
						continue
					}
					Ok(h) => h,
				};

				let mut proposer = match env.init(&header) {
					Err(err) => {
						rpc::send_result(sender, Err(Error::ProposerError(format!("{}", err))));
						continue
					}
					Ok(p) => p,
				};

				let id = match inherent_data_providers.create_inherent_data() {
					Err(err) => {
						rpc::send_result(sender, Err(err.into()));
						continue
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
							finalized: false,
							auxiliary: Vec::new(),
							fork_choice: ForkChoiceStrategy::LongestChain,
							allow_missing_state: false,
							import_existing: false,
						}, header)
					}
					Err(err) => {
						rpc::send_result(sender, Err(Error::ProposerError(format!("{}", err))));
						continue
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
			EngineCommand::FinalizeBlock { hash, sender } => {
				// TODO(seun): Justification support?
				match back_end.finalize_block(BlockId::Hash(hash), None) {
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
		}
	}
}

pub async fn run_instant_seal<B, CB, H, E, A, C>(
	block_import: BoxBlockImport<B>,
	env: E,
	back_end: Arc<CB>,
	basic_pool: Arc<BasicPool<A, B>>,
	select_chain: C,
	inherent_data_providers: inherents::InherentDataProviders,
)
	where
		A: txpool::ChainApi<Block=B, Hash=<B as BlockT>::Hash> + 'static,
		B: BlockT + 'static,
		CB: ClientBackend<B, H> + 'static,
		E: Environment<B> + 'static,
		E::Error: std::fmt::Display,
		<E::Proposer as Proposer<B>>::Error: std::fmt::Display,
		H: Hasher<Out=<B as BlockT>::Hash>,
		C: SelectChain<B> + 'static
{
	// instant-seal creates blocks as soon as transactions are imported
	// into the transaction pool.
	let seal_block_channel = basic_pool.pool().import_notification_stream()
		.map(|_| {
			EngineCommand::SealNewBlock {
				create_empty: false,
				parent_hash: None,
				sender: None,
			}
		});

	run_manual_seal(
		block_import,
		env,
		back_end,
		basic_pool,
		seal_block_channel,
		select_chain,
		inherent_data_providers,
	).await
}

#[cfg(test)]
mod tests {
	use super::*;
	use test_client::{DefaultTestClientBuilderExt, TestClientBuilderExt, AccountKeyring::*};
	use transaction_pool::{
		BasicPool,
		txpool::Options,
		testing::*,
	};
	use consensus_common::ImportedAux;
	use txpool_api::TransactionPool;
	use client::LongestChain;
	use inherents::InherentDataProviders;
	use test_client;
	use basic_authorship::ProposerFactory;

	#[tokio::test]
	async fn instant_seal() {
		let builder = test_client::TestClientBuilder::new();
		let backend = builder.backend();
		let client = Arc::new(builder.build());
		let select_chain = LongestChain::new(backend.clone());
		let inherent_data_providers = InherentDataProviders::new();
		let pool = Arc::new(BasicPool::new(Options::default(), TestApi::default()));
		let env = ProposerFactory {
			transaction_pool: pool.clone(),
			client: client.clone(),
		};
		// this test checks that blocks are created as soon as transactions are imported into the pool.
		let (sender, receiver) = futures::channel::oneshot::channel();
		let mut sender = Arc::new(Some(sender));
		let stream = pool.pool().import_notification_stream()
			.map(move |_| {
				// we're only going to submit one tx so this fn will only be called once.
				let mut_sender =  Arc::get_mut(&mut sender).unwrap();
				let sender = std::mem::replace(mut_sender, None);
				EngineCommand::SealNewBlock {
					create_empty: false,
					parent_hash: None,
					sender
				}
			});
		let future = run_manual_seal(
			Box::new(client.clone()),
			env,
			backend.clone(),
			pool.clone(),
			stream,
			select_chain,
			inherent_data_providers,
		);
		std::thread::spawn(|| {
			let mut rt = tokio::runtime::Runtime::new().unwrap();
			// spawn the background authorship task
			rt.block_on(future);
		});
		// submit a transaction to pool.
		let result = pool.submit_one(&BlockId::Number(0), uxt(Alice, 209)).await;
		// assert that it was successfully imported
		assert!(result.is_ok());
		// assert that the background task returns ok
		let created_block = receiver.await.unwrap().unwrap();
		assert_eq!(
			created_block,
			CreatedBlock {
				hash: created_block.hash.clone(),
				aux: ImportedAux {
					header_only: false,
					clear_justification_requests: false,
					needs_justification: false,
					bad_justification: false,
					needs_finality_proof: false,
					is_new_best: true,
				}
			}
		);
		// assert that there's a new block in the db.
		assert!(backend.blockchain().header(BlockId::Number(1)).unwrap().is_some())
	}

	#[tokio::test]
	async fn manual_seal_and_finalization() {
		let builder = test_client::TestClientBuilder::new();
		let backend = builder.backend();
		let client = Arc::new(builder.build());
		let select_chain = LongestChain::new(backend.clone());
		let inherent_data_providers = InherentDataProviders::new();
		let pool = Arc::new(BasicPool::new(Options::default(), TestApi::default()));
		let env = ProposerFactory {
			transaction_pool: pool.clone(),
			client: client.clone(),
		};
		// this test checks that blocks are created as soon as an engine command is sent over the stream.
		let (mut sink, stream) = futures::channel::mpsc::channel(1024);
		let future = run_manual_seal(
			Box::new(client.clone()),
			env,
			backend.clone(),
			pool.clone(),
			stream,
			select_chain,
			inherent_data_providers,
		);
		std::thread::spawn(|| {
			let mut rt = tokio::runtime::Runtime::new().unwrap();
			// spawn the background authorship task
			rt.block_on(future);
		});
		// submit a transaction to pool.
		let result = pool.submit_one(&BlockId::Number(0), uxt(Alice, 209)).await;
		// assert that it was successfully imported
		assert!(result.is_ok());
		let (tx, rx) = futures::channel::oneshot::channel();
		sink.send(EngineCommand::SealNewBlock {
			parent_hash: None,
			sender: Some(tx),
			create_empty: false,
		}).await.unwrap();
		let created_block = rx.await.unwrap().unwrap();

		// assert that the background task returns ok
		assert_eq!(
			created_block,
			CreatedBlock {
				hash: created_block.hash.clone(),
				aux: ImportedAux {
					header_only: false,
					clear_justification_requests: false,
					needs_justification: false,
					bad_justification: false,
					needs_finality_proof: false,
					is_new_best: true,
				}
			}
		);
		// assert that there's a new block in the db.
		let header = backend.blockchain().header(BlockId::Number(1)).unwrap().unwrap();
		let (tx, rx) = futures::channel::oneshot::channel();
		sink.send(EngineCommand::FinalizeBlock {
			sender: Some(tx),
			hash: header.hash()
		}).await.unwrap();
		// assert that the background task returns ok
		assert_eq!(rx.await.unwrap().unwrap(), ());
	}
	// TODO: test with fork blocks
}
