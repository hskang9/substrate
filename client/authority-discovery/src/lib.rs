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

#![warn(missing_docs)]

//! Substrate authority discovery.
//!
//! This crate enables Substrate authorities to directly connect to other authorities.
//! [`AuthorityDiscovery`] implements the Future trait. By polling [`AuthorityDiscovery`] an
//! authority:
//!
//!
//! 1. **Makes itself discoverable**
//!
//!    1. Retrieves its external addresses.
//!
//!    2. Adds its network peer id to the addresses.
//!
//!    3. Signs the above.
//!
//!    4. Puts the signature and the addresses on the libp2p Kademlia DHT.
//!
//!
//! 2. **Discovers other authorities**
//!
//!    1. Retrieves the current set of authorities.
//!
//!    2. Starts DHT queries for the ids of the authorities.
//!
//!    3. Validates the signatures of the retrieved key value pairs.
//!
//!    4. Adds the retrieved external addresses as priority nodes to the peerset.
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::task::{Context, Poll};
use futures::{Future, FutureExt, Stream, StreamExt};
use futures_timer::Delay;

use sp_authority_discovery::{
	AuthorityDiscoveryApi, AuthorityId, AuthoritySignature, AuthorityPair
};
use sc_client_api::blockchain::HeaderBackend;
use codec::{Decode, Encode};
use error::{Error, Result};
use log::{debug, error, log_enabled, warn};
use libp2p::Multiaddr;
use sc_network::specialization::NetworkSpecialization;
use sc_network::{DhtEvent, ExHashT, NetworkStateInfo};
use sp_core::crypto::{key_types, Pair};
use sp_core::traits::BareCryptoStorePtr;
use prost::Message;
use sp_runtime::generic::BlockId;
use sp_runtime::traits::{Block as BlockT, ProvideRuntimeApi};

#[cfg(test)]
mod tests;

mod error;
/// Dht payload schemas generated from Protobuf definitions via Prost crate in build.rs.
mod schema {
	include!(concat!(env!("OUT_DIR"), "/authority_discovery.rs"));
}

type Interval = Box<dyn Stream<Item = ()> + Unpin + Send + Sync>;

/// Upper bound estimation on how long one should wait before accessing the Kademlia DHT.
const LIBP2P_KADEMLIA_BOOTSTRAP_TIME: Duration = Duration::from_secs(30);

/// Name of the Substrate peerset priority group for authorities discovered through the authority
/// discovery module.
const AUTHORITIES_PRIORITY_GROUP_NAME: &'static str = "authorities";

/// The maximum number of sentry node public addresses that we accept per authority.
///
/// Everything above this threshold should be dropped to prevent a single authority from filling up
/// our peer set priority group.
const MAX_NUM_SENTRY_ADDRESSES_PER_AUTHORITY: usize = 5;

/// An `AuthorityDiscovery` makes a given authority discoverable and discovers other authorities.
pub struct AuthorityDiscovery<Client, Network, Block>
where
	Block: BlockT + 'static,
	Network: NetworkProvider,
	Client: ProvideRuntimeApi + Send + Sync + 'static + HeaderBackend<Block>,
	<Client as ProvideRuntimeApi>::Api: AuthorityDiscoveryApi<Block>,
{
	client: Arc<Client>,

	network: Arc<Network>,
	/// List of sentry node public addresses.
	//
	// There are 3 states:
	//   - None: No addresses were specified.
	//   - Some(vec![]): Addresses were specified, but none could be parsed as proper
	//     Multiaddresses.
	//   - Some(vec![a, b, c, ...]): Valid addresses were specified.
	sentry_nodes: Option<Vec<Multiaddr>>,
	/// Channel we receive Dht events on.
	dht_event_rx: Pin<Box<dyn Stream<Item = DhtEvent> + Send>>,

	key_store: BareCryptoStorePtr,

	/// Interval to be proactive, publishing own addresses.
	publish_interval: Interval,
	/// Interval on which to query for addresses of other authorities.
	query_interval: Interval,

	/// The network peerset interface for priority groups lets us only set an entire group, but we
	/// retrieve the addresses of other authorities one by one from the network. To use the peerset
	/// interface we need to cache the addresses and always overwrite the entire peerset priority
	/// group. To ensure this map doesn't grow indefinitely `purge_old_authorities_from_cache`
	/// function is called each time we add a new entry.
	address_cache: HashMap<AuthorityId, Vec<Multiaddr>>,

	phantom: PhantomData<Block>,
}

impl<Client, Network, Block> AuthorityDiscovery<Client, Network, Block>
where
	Block: BlockT + Unpin + 'static,
	Network: NetworkProvider,
	Client: ProvideRuntimeApi + Send + Sync + 'static + HeaderBackend<Block>,
	<Client as ProvideRuntimeApi>::Api: AuthorityDiscoveryApi<Block, Error = sp_blockchain::Error>,
	Self: Future<Output = ()>,
{
	/// Return a new authority discovery.
	///
	/// Note: When specifying `sentry_nodes` this module will not advertise the public addresses of
	/// the node itself but only the public addresses of its sentry nodes.
	pub fn new(
		client: Arc<Client>,
		network: Arc<Network>,
		sentry_nodes: Vec<String>,
		key_store: BareCryptoStorePtr,
		dht_event_rx: Pin<Box<dyn Stream<Item = DhtEvent> + Send>>,
	) -> Self {
		// Kademlia's default time-to-live for Dht records is 36h, republishing records every 24h.
		// Given that a node could restart at any point in time, one can not depend on the
		// republishing process, thus publishing own external addresses should happen on an interval
		// < 36h.
		let publish_interval = interval_at(
			Instant::now() + LIBP2P_KADEMLIA_BOOTSTRAP_TIME,
			Duration::from_secs(12 * 60 * 60),
		);

		// External addresses of other authorities can change at any given point in time. The
		// interval on which to query for external addresses of other authorities is a trade off
		// between efficiency and performance.
		let query_interval = interval_at(
			Instant::now() + LIBP2P_KADEMLIA_BOOTSTRAP_TIME,
			Duration::from_secs(10 * 60),
		);

		let sentry_nodes = if !sentry_nodes.is_empty() {
			let addrs = sentry_nodes.into_iter().filter_map(|a| match a.parse() {
				Ok(addr) => Some(addr),
				Err(e) => {
					error!(
						target: "sub-authority-discovery",
						"Failed to parse sentry node public address '{:?}', continuing anyways.", e,
					);
					None
				}
			}).collect::<Vec<Multiaddr>>();

			if addrs.len() > MAX_NUM_SENTRY_ADDRESSES_PER_AUTHORITY {
				warn!(
					target: "sub-authority-discovery",
					"More than MAX_NUM_SENTRY_ADDRESSES_PER_AUTHORITY ({:?}) were specified. Other \
					nodes will likely ignore the remainder.",
					MAX_NUM_SENTRY_ADDRESSES_PER_AUTHORITY,
				);
			}

			Some(addrs)
		} else {
			None
		};


		let address_cache = HashMap::new();

		AuthorityDiscovery {
			client,
			network,
			sentry_nodes,
			dht_event_rx,
			key_store,
			publish_interval,
			query_interval,
			address_cache,
			phantom: PhantomData,
		}
	}

	/// Publish either our own or if specified the public addresses of our sentry nodes.
	fn publish_ext_addresses(&mut self) -> Result<()> {
		let addresses = match &self.sentry_nodes {
			Some(addrs) => addrs.clone().into_iter()
				.map(|a| a.to_vec())
				.collect(),
			None => self.network.external_addresses()
				.into_iter()
				.map(|a| a.with(libp2p::core::multiaddr::Protocol::P2p(
					self.network.local_peer_id().into(),
				)))
				.map(|a| a.to_vec())
				.collect(),
		};

		let mut serialized_addresses = vec![];
		schema::AuthorityAddresses { addresses }
			.encode(&mut serialized_addresses)
			.map_err(Error::EncodingProto)?;

		for key in self.get_priv_keys_within_authority_set()?.into_iter() {
			let signature = key.sign(&serialized_addresses);

			let mut signed_addresses = vec![];
			schema::SignedAuthorityAddresses {
				addresses: serialized_addresses.clone(),
				signature: signature.encode(),
			}
			.encode(&mut signed_addresses)
				.map_err(Error::EncodingProto)?;

			self.network.put_value(
				hash_authority_id(key.public().as_ref())?,
				signed_addresses,
			);
		}

		Ok(())
	}

	fn request_addresses_of_others(&mut self) -> Result<()> {
		let id = BlockId::hash(self.client.info().best_hash);

		let authorities = self
			.client
			.runtime_api()
			.authorities(&id)
			.map_err(Error::CallingRuntime)?;

		for authority_id in authorities.iter() {
			self.network
				.get_value(&hash_authority_id(authority_id.as_ref())?);
		}

		Ok(())
	}

	fn handle_dht_events(&mut self, cx: &mut Context) -> Result<()> {
		while let Poll::Ready(Some(event)) = self.dht_event_rx.poll_next_unpin(cx) {
			match event {
				DhtEvent::ValueFound(v) => {
					if log_enabled!(log::Level::Debug) {
						let hashes = v.iter().map(|(hash, _value)| hash.clone());
						debug!(
							target: "sub-authority-discovery",
							"Value for hash '{:?}' found on Dht.", hashes,
						);
					}

					self.handle_dht_value_found_event(v)?;
				}
				DhtEvent::ValueNotFound(hash) => debug!(
					target: "sub-authority-discovery",
					"Value for hash '{:?}' not found on Dht.", hash
				),
				DhtEvent::ValuePut(hash) => debug!(
					target: "sub-authority-discovery",
					"Successfully put hash '{:?}' on Dht.", hash),
				DhtEvent::ValuePutFailed(hash) => warn!(
					target: "sub-authority-discovery",
					"Failed to put hash '{:?}' on Dht.", hash
				),
			}
		}

		Ok(())
	}

	fn handle_dht_value_found_event(
		&mut self,
		values: Vec<(libp2p::kad::record::Key, Vec<u8>)>,
	) -> Result<()> {
		debug!(target: "sub-authority-discovery", "Got Dht value from network.");

		let block_id = BlockId::hash(self.client.info().best_hash);

		// From the Dht we only get the hashed authority id. In order to retrieve the actual
		// authority id and to ensure it is actually an authority, we match the hash against the
		// hash of the authority id of all other authorities.
		let authorities = self.client.runtime_api().authorities(&block_id)?;
		self.purge_old_authorities_from_cache(&authorities);

		let authorities = authorities
			.into_iter()
			.map(|id| hash_authority_id(id.as_ref()).map(|h| (h, id)))
			.collect::<Result<HashMap<_, _>>>()?;

		for (key, value) in values.iter() {
			// Check if the event origins from an authority in the current authority set.
			let authority_id: &AuthorityId = authorities
				.get(key)
				.ok_or(Error::MatchingHashedAuthorityIdWithAuthorityId)?;

			let schema::SignedAuthorityAddresses {
				signature,
				addresses,
			} = schema::SignedAuthorityAddresses::decode(value).map_err(Error::DecodingProto)?;
			let signature = AuthoritySignature::decode(&mut &signature[..])
				.map_err(Error::EncodingDecodingScale)?;

			if !AuthorityPair::verify(&signature, &addresses, authority_id) {
				return Err(Error::VerifyingDhtPayload);
			}

			let mut addresses: Vec<libp2p::Multiaddr> = schema::AuthorityAddresses::decode(addresses)
				.map(|a| a.addresses)
				.map_err(Error::DecodingProto)?
				.into_iter()
				.map(|a| a.try_into())
				.collect::<std::result::Result<_, _>>()
				.map_err(Error::ParsingMultiaddress)?;

			if addresses.len() > MAX_NUM_SENTRY_ADDRESSES_PER_AUTHORITY {
				warn!(
					target: "sub-authority-discovery",
					"Got more than MAX_NUM_SENTRY_ADDRESSES_PER_AUTHORITY ({:?}) for Authority \
					'{:?}' from DHT, dropping the remainder.",
					MAX_NUM_SENTRY_ADDRESSES_PER_AUTHORITY,	authority_id,
				);
				addresses = addresses.into_iter()
					.take(MAX_NUM_SENTRY_ADDRESSES_PER_AUTHORITY)
					.collect();
			}

			self.address_cache.insert(authority_id.clone(), addresses);
		}

		// Let's update the peerset priority group with all the addresses we have in our cache.

		let addresses = HashSet::from_iter(
			self.address_cache
				.iter()
				.map(|(_peer_id, addresses)| addresses.clone())
				.flatten(),
		);

		debug!(
			target: "sub-authority-discovery",
			"Applying priority group {:#?} to peerset.", addresses,
		);
		self.network
			.set_priority_group(AUTHORITIES_PRIORITY_GROUP_NAME.to_string(), addresses)
			.map_err(Error::SettingPeersetPriorityGroup)?;

		Ok(())
	}

	fn purge_old_authorities_from_cache(&mut self, current_authorities: &Vec<AuthorityId>) {
		self.address_cache
			.retain(|peer_id, _addresses| current_authorities.contains(peer_id))
	}

	/// Retrieve all local authority discovery private keys that are within the current authority
	/// set.
	fn get_priv_keys_within_authority_set(&mut self) -> Result<Vec<AuthorityPair>> {
		let keys = self.get_own_public_keys_within_authority_set()?
			.into_iter()
			.map(std::convert::Into::into)
			.filter_map(|pub_key| {
				self.key_store.read().sr25519_key_pair(key_types::AUTHORITY_DISCOVERY, &pub_key)
			})
			.map(std::convert::Into::into)
			.collect();

		Ok(keys)
	}

	/// Retrieve our public keys within the current authority set.
	//
	// A node might have multiple authority discovery keys within its keystore, e.g. an old one and
	// one for the upcoming session. In addition it could be participating in the current authority
	// set with two keys. The function does not return all of the local authority discovery public
	// keys, but only the ones intersecting with the current authority set.
	fn get_own_public_keys_within_authority_set(&mut self) -> Result<HashSet<AuthorityId>> {
		let local_pub_keys = self.key_store
			.read()
			.sr25519_public_keys(key_types::AUTHORITY_DISCOVERY)
			.into_iter()
			.collect::<HashSet<_>>();

		let id = BlockId::hash(self.client.info().best_hash);
		let current_authorities = self
			.client
			.runtime_api()
			.authorities(&id)
			.map_err(Error::CallingRuntime)?
			.into_iter()
			.map(std::convert::Into::into)
			.collect::<HashSet<_>>();

		let intersection = local_pub_keys.intersection(&current_authorities)
			.cloned()
			.map(std::convert::Into::into)
			.collect();

		Ok(intersection)
	}
}

impl<Client, Network, Block> Future for AuthorityDiscovery<Client, Network, Block>
where
	Block: BlockT + Unpin + 'static,
	Network: NetworkProvider,
	Client: ProvideRuntimeApi + Send + Sync + 'static + HeaderBackend<Block>,
	<Client as ProvideRuntimeApi>::Api: AuthorityDiscoveryApi<Block, Error = sp_blockchain::Error>,
{
	type Output = ();

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		let mut inner = || -> Result<()> {
			// Process incoming events before triggering new ones.
			self.handle_dht_events(cx)?;

			if let Poll::Ready(_) = self.publish_interval.poll_next_unpin(cx) {
				// Make sure to call interval.poll until it returns Async::NotReady once. Otherwise,
				// in case one of the function calls within this block do a `return`, we don't call
				// `interval.poll` again and thereby the underlying Tokio task is never registered
				// with Tokio's Reactor to be woken up on the next interval tick.
				while let Poll::Ready(_) = self.publish_interval.poll_next_unpin(cx) {}

				self.publish_ext_addresses()?;
			}

			if let Poll::Ready(_) = self.query_interval.poll_next_unpin(cx) {
				// Make sure to call interval.poll until it returns Async::NotReady once. Otherwise,
				// in case one of the function calls within this block do a `return`, we don't call
				// `interval.poll` again and thereby the underlying Tokio task is never registered
				// with Tokio's Reactor to be woken up on the next interval tick.
				while let Poll::Ready(_) = self.query_interval.poll_next_unpin(cx) {}

				self.request_addresses_of_others()?;
			}

			Ok(())
		};

		match inner() {
			Ok(()) => {}
			Err(e) => error!(target: "sub-authority-discovery", "Poll failure: {:?}", e),
		};

		// Make sure to always return NotReady as this is a long running task with the same lifetime
		// as the node itself.
		Poll::Pending
	}
}

/// NetworkProvider provides AuthorityDiscovery with all necessary hooks into the underlying
/// Substrate networking. Using this trait abstraction instead of NetworkService directly is
/// necessary to unit test AuthorityDiscovery.
pub trait NetworkProvider: NetworkStateInfo {
	/// Modify a peerset priority group.
	fn set_priority_group(
		&self,
		group_id: String,
		peers: HashSet<libp2p::Multiaddr>,
	) -> std::result::Result<(), String>;

	/// Start putting a value in the Dht.
	fn put_value(&self, key: libp2p::kad::record::Key, value: Vec<u8>);

	/// Start getting a value from the Dht.
	fn get_value(&self, key: &libp2p::kad::record::Key);
}

impl<B, S, H> NetworkProvider for sc_network::NetworkService<B, S, H>
where
	B: BlockT + 'static,
	S: NetworkSpecialization<B>,
	H: ExHashT,
{
	fn set_priority_group(
		&self,
		group_id: String,
		peers: HashSet<libp2p::Multiaddr>,
	) -> std::result::Result<(), String> {
		self.set_priority_group(group_id, peers)
	}
	fn put_value(&self, key: libp2p::kad::record::Key, value: Vec<u8>) {
		self.put_value(key, value)
	}
	fn get_value(&self, key: &libp2p::kad::record::Key) {
		self.get_value(key)
	}
}

fn hash_authority_id(id: &[u8]) -> Result<libp2p::kad::record::Key> {
	libp2p::multihash::encode(libp2p::multihash::Hash::SHA2256, id)
		.map(|k| libp2p::kad::record::Key::new(&k))
		.map_err(Error::HashingAuthorityId)
}

fn interval_at(start: Instant, duration: Duration) -> Interval {
	let stream = futures::stream::unfold(start, move |next| {
		let time_until_next =  next.saturating_duration_since(Instant::now());

		Delay::new(time_until_next).map(move |_| Some(((), next + duration)))
	});

	Box::new(stream)
}
