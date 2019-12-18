// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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

//! Utilities for testing transaction pool
use super::*;
use codec::Encode;
use txpool::{self, Pool};
use substrate_test_runtime_client::{
	runtime::{AccountId, Index, Transfer, Extrinsic, Block, Hash},
	AccountKeyring
};
use sp_runtime::{
	generic::{BlockId},
	traits::{Hash as HashT, BlakeTwo256},
	transaction_validity::{TransactionValidity, ValidTransaction},
};

/// Transaction pool api that performs some custom or no validation.
pub struct TestApi {
	/// custom validation function to run when importing transactions.
	pub modifier: Box<dyn Fn(&mut ValidTransaction) + Send + Sync>,
}


impl TestApi {
	/// Creates a instance of TestApi that performs no validation.
	pub fn default() -> Self {
		TestApi {
			modifier: Box::new(|_| {}),
		}
	}
}

impl txpool::ChainApi for TestApi {
	type Block = Block;
	type Hash = Hash;
	type Error = error::Error;
	type ValidationFuture = futures::future::Ready<error::Result<TransactionValidity>>;

	fn validate_transaction(
		&self,
		at: &BlockId<Self::Block>,
		uxt: txpool::ExtrinsicFor<Self>,
	) -> Self::ValidationFuture {
		let expected = index(at);
		let requires = if expected == uxt.transfer().nonce {
			vec![]
		} else {
			vec![vec![uxt.transfer().nonce as u8 - 1]]
		};
		let provides = vec![vec![uxt.transfer().nonce as u8]];

		let mut validity = ValidTransaction {
			priority: 1,
			requires,
			provides,
			longevity: 64,
			propagate: true,
		};

		(self.modifier)(&mut validity);

		futures::future::ready(Ok(
			Ok(validity)
		))
	}

	fn block_id_to_number(&self, at: &BlockId<Self::Block>) -> error::Result<Option<txpool::NumberFor<Self>>> {
		Ok(Some(number_of(at)))
	}

	fn block_id_to_hash(&self, at: &BlockId<Self::Block>) -> error::Result<Option<txpool::BlockHash<Self>>> {
		Ok(match at {
			BlockId::Hash(x) => Some(x.clone()),
			_ => Some(Default::default()),
		})
	}

	fn hash_and_length(&self, ex: &txpool::ExtrinsicFor<Self>) -> (Self::Hash, usize) {
		let encoded = ex.encode();
		(BlakeTwo256::hash(&encoded), encoded.len())
	}

}

/// generate nonce to be used with testing TestApi
pub fn index(at: &BlockId<Block>) -> u64 {
	209 + number_of(at)
}

fn number_of(at: &BlockId<Block>) -> u64 {
	match at {
		BlockId::Number(n) => *n as u64,
		_ => 0,
	}
}

/// generates a transfer extrinsic, given a keyring and a nonce.
pub fn uxt(who: AccountKeyring, nonce: Index) -> Extrinsic {
	let transfer = Transfer {
		from: who.into(),
		to: AccountId::default(),
		nonce,
		amount: 1,
	};
	let signature = transfer.using_encoded(|e| who.sign(e));
	Extrinsic::Transfer(transfer, signature.into())
}

/// creates a transaction pool with the TestApi ChainApi
pub fn pool() -> Pool<TestApi> {
	Pool::new(Default::default(), TestApi::default())
}
