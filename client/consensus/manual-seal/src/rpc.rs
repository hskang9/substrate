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

//! RPC interface for the ManualSeal Engine.
use consensus_common::ImportedAux;
use jsonrpc_core::{Error, ErrorCode};
use jsonrpc_derive::rpc;
use futures::{
	channel::{
		mpsc::{self, SendError},
		oneshot,
	},
	TryFutureExt,
	SinkExt
};
use serde::{Deserialize, Serialize};
use sp_runtime::Justification;

/// Future's type for jsonrpc
type FutureResult<T> = Box<dyn jsonrpc_core::futures::Future<Item = T, Error = Error> + Send>;
/// sender passed to the authorship task to report errors or successes.
pub type Sender<T> = Option<oneshot::Sender<std::result::Result<T, crate::Error>>>;

/// Message sent to the background authorship task, usually by RPC.
pub enum EngineCommand<Hash> {
	/// Tells the engine to propose a new block
	///
	/// if create_empty == true, it will create empty blocks if there are no transactions
	/// in the transaction pool.
	///
	/// if finalize == true, the block will be instantly finalized.
	SealNewBlock {
		/// if true, empty blocks(without extrinsics) will be created.
		/// otherwise, will return Error::EmptyTransactionPool.
		create_empty: bool,
		/// instantly finalize this block?
		finalize: bool,
		/// specify the parent hash of the about-to-created block
		parent_hash: Option<Hash>,
		/// sender to report errors/success to the rpc.
		sender: Sender<CreatedBlock<Hash>>,
	},
	/// Tells the engine to finalize the block with the supplied hash
	FinalizeBlock {
		/// hash of the block
		hash: Hash,
		/// sender to report errors/success to the rpc.
		sender: Sender<()>,
		/// finalization justification
		justification: Option<Justification>
	}
}

/// RPC trait that provides methods for interacting with the manual-seal authorship task over rpc.
#[rpc]
pub trait ManualSealApi<Hash> {
	/// Instructs the manual-seal authorship task to create a new block
	#[rpc(name = "engine_createBlock")]
	fn create_block(
		&self,
		create_empty: bool,
		finalize: bool,
		parent_hash: Option<Hash>
	) -> FutureResult<CreatedBlock<Hash>>;

	/// Instructs the manual-seal authorship task to finalize a block
	#[rpc(name = "engine_finalizeBlock")]
	fn finalize_block(
		&self,
		hash: Hash,
		justification: Option<Justification>
	) -> FutureResult<()>;
}

/// A struct that implements the [`ManualSealApi`].
pub struct ManualSeal<Hash> {
	import_block_channel: mpsc::Sender<EngineCommand<Hash>>,
}

/// return type of `engine_createBlock`
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CreatedBlock<Hash> {
	/// hash of the created block.
	pub hash: Hash,
	/// some extra details about the import operation
	pub aux: ImportedAux
}

impl<Hash> ManualSeal<Hash> {
	/// Create new `ManualSeal` with the given reference to the client.
	pub fn new(import_block_channel: mpsc::Sender<EngineCommand<Hash>>) -> Self {
		Self { import_block_channel }
	}
}

impl<Hash: Send + 'static> ManualSealApi<Hash> for ManualSeal<Hash> {
	fn create_block(
		&self,
		create_empty: bool,
		finalize: bool,
		parent_hash: Option<Hash>
	) -> FutureResult<CreatedBlock<Hash>> {
		let mut sink = self.import_block_channel.clone();
		let future = async move {
			let (sender, receiver) = oneshot::channel();
			let result = sink.send(
				EngineCommand::SealNewBlock {
					create_empty,
					finalize,
					parent_hash,
					sender: Some(sender),
				}
			).await;
			result.map_err(map_error)?;

			match receiver.await {
				// all good
				Ok(Ok(block)) => Ok(block),
				// error from the authorship task
				Ok(Err(e)) => {
					Err(Error {
						code: ErrorCode::ServerError(500),
						message: format!("{}", e),
						data: None,
					})
				}
				// channel has been dropped
				Err(_) => {
					Err(Error {
						code: ErrorCode::ServerError(500),
						message: "Consensus process is terminating".into(),
						data: None,
					})
				}
			}
		};

		Box::new(Box::pin(future).compat())
	}

	fn finalize_block(&self, hash: Hash, justification: Option<Justification>) -> FutureResult<()> {
		let mut sink = self.import_block_channel.clone();
		let future = async move {
			let (sender, receiver) = oneshot::channel();
			let result = sink.send(
				EngineCommand::FinalizeBlock { hash, sender: Some(sender), justification }
			).await;
			result.map_err(map_error)?;

			match receiver.await {
				// all good
				Ok(Ok(())) => Ok(()),
				// error from the authorship task
				Ok(Err(e)) => {
					Err(Error {
						code: ErrorCode::ServerError(500),
						message: format!("{}", e),
						data: None
					})
				}
				// channel has been dropped
				Err(_) => {
					Err(Error {
						code: ErrorCode::ServerError(500),
						message: "Consensus process is terminating".into(),
						data: None
					})
				}
			}
		};

		Box::new(Box::pin(future).compat())
	}
}

/// convert a futures::mpsc::SendError to jsonrpc::Error
fn map_error(error: SendError) -> Error {
	if error.is_disconnected() {
		log::warn!("Received sealing request but Manual Sealing task has been dropped");
	}

	Error {
		code: ErrorCode::ServerError(500),
		message: "Consensus process is terminating".into(),
		data: None
	}
}

/// report any errors or successes encountered by the authorship task back
/// to the rpc
pub fn send_result<T: std::fmt::Debug>(
	sender: Sender<T>,
	result: std::result::Result<T, crate::Error>
) {
	sender.map(|sender| {
		sender.send(result)
			.expect("receiving end isn't dropped until it receives a message; qed")
	});
}
