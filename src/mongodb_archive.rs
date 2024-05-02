/// An implementation of an archive datastore that uses MongoDB as its backend. Within the
/// database, this backend stores account data and transaction data as separate document
/// collections as defined by the [ACCOUNT_COLLECTION] and [TRANSACTION_COLLECTION] constants. It
/// uses the datastore name passed in as the name of the MongoDB database to archive to/from.
use crate::{ArchiveBackend, ArchiveRecordType};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::stream::TryStreamExt;
use log::debug;
use mongodb::{bson::doc, options::ClientOptions, Client, Collection};
use serde::{de::DeserializeOwned, Serialize};
use std::borrow::Borrow;

/// MongoDB collection name for storing account data
const ACCOUNT_COLLECTION: &str = "accounts";
/// MongoDB collection name for storing trasnaction data
const TRANSACTION_COLLECTION: &str = "transaction_data";

#[derive(Debug)]
pub struct MongoDBBackend {
    pub uri: String,
    pub datastore: String,
}

#[async_trait]
impl ArchiveBackend for MongoDBBackend {
    /// Take any blob, as long as it can be serialised to BSON, and insert it into the relevant
    /// collection.
    async fn create<T: Serialize>(&mut self, rec_type: ArchiveRecordType, rec: T) -> Result<String>
    where
        T: Borrow<T> + std::marker::Send + std::marker::Sync,
    {
        // We call connect each time rather than taking a handle and holding onto it. The Rust
        // driver for MongoDB handles connection pooling and is likely to do a better job at us of
        // managing connections and retries than us. The connect call below will generally be a
        // no-op unless the connection was dropped.

        // Set DB client options, including URI and then create client handle
        let options = ClientOptions::parse(&self.uri)
            .await
            .context(format!("Failed to parse MongoDB URI: '{}'", self.uri))?;

        let client =
            Client::with_options(options).context("Failed to set MongoDB client options")?;

        // Associate with a specific database
        let db = client.database(&self.datastore);

        // Retrieve the relevant collection handle.
        let collection: Collection<T>;
        if let ArchiveRecordType::Account = rec_type {
            collection = db.collection(ACCOUNT_COLLECTION);
        } else if let ArchiveRecordType::TransactionBatch = rec_type {
            collection = db.collection(TRANSACTION_COLLECTION);
        } else {
            panic!("Invalid archive record type");
        }

        // Now insert the record that was passed in....
        let res = collection
            .insert_one(rec, None)
            .await
            .context("Failed to insert document")?;

        // Here we should log the doc ID
        debug!("Inserted {}", res.inserted_id.to_string());

        Ok(res.inserted_id.to_owned().to_string())
    }

    /// Query data store for all records matching a specific attribute. For example all accounts
    /// in the [ACCOUNT_COLLECTION] table with a specific account ID.
    async fn find_all<T: DeserializeOwned>(&mut self, rec_type: ArchiveRecordType) -> Result<Vec<T>>
    where
        T: Borrow<T> + std::marker::Send + std::marker::Sync + std::clone::Clone + Unpin,
    {
        // We call connect each time rather than taking a handle and holding onto it. The Rust
        // driver for MongoDB handles connection pooling and is likely to do a better job at us of
        // managing connections and retries than us. The connect call below will generally be a
        // no-op unless the connection was dropped.

        // Set DB client options, including URI and then create client handle
        let options = ClientOptions::parse(&self.uri)
            .await
            .context(format!("Failed to parse MongoDB URI: '{}'", self.uri))?;

        let client =
            Client::with_options(options).context("Failed to set MongoDB client options")?;

        // Associate with a specific database
        let db = client.database(&self.datastore);

        // Retrieve the relevant collection handle.
        let collection: Collection<T>;
        if let ArchiveRecordType::Account = rec_type {
            collection = db.collection(ACCOUNT_COLLECTION);
        } else if let ArchiveRecordType::TransactionBatch = rec_type {
            collection = db.collection(TRANSACTION_COLLECTION);
        } else {
            panic!("Invalid archive record type");
        }

        let filter = doc! { "_id": "$exists" };

        // Now insert the record that was passed in....
        let cursor = collection
            .find(filter, None)
            .await
            .context("Failed to find documents")?;

        // TODO: now do stuff with the returned Cursor...
        let ret: Vec<T> = cursor.try_collect().await?;
        //while cursor.advance().await? {
        //println!("Doc: {:?}", cursor.deserialize_current()?);
        //let val: T = cursor.deserialize_current()?;
        //ret.push(val.clone());
        //}
        //while let Some(doc) = cursor.try_next().await? {
        //    dbg!(doc);
        //}
        // TODO: just returns the filter string, not the results
        Ok(ret)
    }
}
