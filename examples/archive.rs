use anyhow::Result;
use lasr_archive::{ArchiveBackends, ArchiveRecordType, ArchiveStoreBuilder};
use serde::{Deserialize, Serialize};

const MONGODB_SECRET: &str = "mongodb+srv://musicalcarrion:2yxiu86tDoOz75UY@testcluster.lq3bpjp.mongodb.net/?retryWrites=true&w=majority&appName=TestCluster";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestDocument {
    thing: String,
    otherthing: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Get a handle on the persistence store
    let mut store = ArchiveStoreBuilder::default()
        .uri(MONGODB_SECRET.to_string())
        .backend(ArchiveBackends::MongoDB)
        .datastore("lasr_archive".to_string())
        .build()?;
    println!("archive store: {}", store);

    let doc = TestDocument {
        thing: "This is a thing".to_string(),
        otherthing: "This is a different thing".to_string(),
    };

    // Write a document that can be serialised to BSON
    let id = store.create(ArchiveRecordType::Account, &doc).await?;
    println!("Returned ID is: {}", id);

    Ok(())
}
