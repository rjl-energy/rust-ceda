//! Process datafiles command

use crate::datastore;

pub async fn process() -> Result<(), crate::error::AppError> {
    let datastore = datastore::DataStore::new();
    let file_properties = datastore.get_file_properties();
    println!("{:#?}", file_properties);

    Ok(())
}