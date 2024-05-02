mod mongodb_archive;

use crate::mongodb_archive::MongoDBBackend;
use anyhow::{Context, Result};
use async_trait::async_trait;
use core::fmt;
use derive_builder::Builder;
use serde::{de::DeserializeOwned, Serialize};
use std::borrow::Borrow;

/// A structure representing an archive datastore
#[derive(Debug, Builder)]
pub struct ArchiveStore {
    /// The backend-specific URI to connect to the archive backend
    uri: String,
    /// Archive backend to use
    backend: ArchiveBackends,
    /// Name of archive datastore
    datastore: String,
}

impl ArchiveStore {
    /// Persists a new archive record of [ArchiveRecordType] in the selected archive backend.
    pub async fn create<T: Serialize>(
        &mut self,
        rec_type: ArchiveRecordType,
        rec: T,
    ) -> Result<String>
    where
        T: Borrow<T> + std::marker::Send + std::marker::Sync,
    {
        match self.backend {
            ArchiveBackends::MongoDB => {
                // Call the MongoDB backend
                let mut backend = MongoDBBackend {
                    uri: self.uri.clone(),
                    datastore: self.datastore.clone(),
                };
                backend
                    .create(rec_type, rec)
                    .await
                    .context("Creating new MongoDB blob.")
            }
        }
    }
    pub async fn find_all<T: DeserializeOwned>(
        &mut self,
        rec_type: ArchiveRecordType,
    ) -> Result<Vec<T>>
    where
        T: Borrow<T> + std::marker::Send + std::marker::Sync + std::clone::Clone + Unpin,
    {
        match self.backend {
            ArchiveBackends::MongoDB => {
                // Call the MongoDB backend
                let mut backend = MongoDBBackend {
                    uri: self.uri.clone(),
                    datastore: self.datastore.clone(),
                };
                backend
                    .find_all(rec_type)
                    .await
                    .context("Retrieving blobs from MongoDB")
            }
        }
    }
}

impl fmt::Display for ArchiveStore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "URI: {}, Backend: {}, Datastore: {}",
            self.uri, self.backend, self.datastore
        )
    }
}

/// A trait that defines an interface for an archive backend to support when implemented.
#[async_trait]
pub trait ArchiveBackend {
    /// Adds a new document to the data store.
    async fn create<T: Serialize>(&mut self, rec_type: ArchiveRecordType, rec: T) -> Result<String>
    where
        T: Borrow<T> + std::marker::Send + std::marker::Sync;
    /// Finds all documents in the data store matching a given attribute's value.
    async fn find_all<T: DeserializeOwned>(
        &mut self,
        rec_type: ArchiveRecordType,
    ) -> Result<Vec<T>>
    where
        T: Borrow<T> + std::marker::Send + std::marker::Sync + std::clone::Clone + Unpin;
}

/// List of possible backends
#[derive(Debug, Clone)]
pub enum ArchiveBackends {
    /// Uses MongoDB as a backend, with a different collection used for each [ArchiveRecordType].
    MongoDB,
}

impl fmt::Display for ArchiveBackends {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ArchiveBackends::MongoDB => write!(f, "MongoDB"),
        }
    }
}

/// An enum representing different types of blobs/records we support archiving. We treat these as
/// being totally opaque within this crate, but may store them separately or slightly differently
/// for performance, indexing, retention and other record-specific criteria.
#[derive(Debug, Clone)]
pub enum ArchiveRecordType {
    Account,
    TransactionBatch,
}
