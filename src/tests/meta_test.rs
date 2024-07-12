#[cfg(test)]
mod tests {
    use crate::consts::{SIZE_OF_U32, SIZE_OF_U64};
    use crate::meta::Meta;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_meta_new() {
        let root = tempdir().unwrap();
        let path = root.path().join("meta_new");

        let metadata = Meta::new(path).await;

        assert!(metadata.is_ok());
        assert_eq!(metadata.as_ref().unwrap().v_log_head, 0);
        assert_eq!(metadata.as_ref().unwrap().v_log_tail, 0);
    }

    #[tokio::test]
    async fn test_set_head_tail() {
        let root = tempdir().unwrap();
        let path = root.path().join("meta_set_tail_head");

        let mut metadata = Meta::new(path).await.unwrap();

        let new_tail = 100;
        let new_head = 50;
        metadata.set_head(new_head);
        metadata.set_tail(new_tail);
        assert_eq!(metadata.v_log_head, new_head);
        assert_eq!(metadata.v_log_tail, new_tail);
    }

    #[tokio::test]
    async fn test_meta_write() {
        let root = tempdir().unwrap();
        let path = root.path().join("meta_new");

        let mut metadata = Meta::new(path).await.unwrap();
        let new_tail = 100;
        let new_head = 50;
        metadata.set_head(new_head);
        metadata.set_tail(new_tail);

        assert!(metadata.write().await.is_ok())
    }

    #[tokio::test]
    async fn test_meta_recover() {
        let root = tempdir().unwrap();
        let path = root.path().join("meta_new");

        let mut metadata = Meta::new(path.to_owned()).await.unwrap();
        let new_tail = 100;
        let new_head = 50;
        metadata.set_head(new_head);
        metadata.set_tail(new_tail);
        metadata.write().await.unwrap();


        let mut recovered_meta = Meta::new(path).await.unwrap();
        let res = recovered_meta.recover().await;
        assert!(res.is_ok());
        assert_eq!(recovered_meta.v_log_head, metadata.v_log_head);
        assert_eq!(recovered_meta.v_log_tail, metadata.v_log_tail);
        assert_eq!(recovered_meta.created_at.timestamp_millis(), metadata.created_at.timestamp_millis());
        assert_eq!(recovered_meta.last_modified.timestamp_millis(), metadata.last_modified.timestamp_millis());
    }

    #[tokio::test]
    async fn test_meta_serialize() {
        let root = tempdir().unwrap();
        let path = root.path().join("meta_serialize");

        let mut metadata = Meta::new(path.to_owned()).await.unwrap();
        let new_tail = 100;
        let new_head = 50;
        metadata.set_head(new_head);
        metadata.set_tail(new_tail);
     

        let expected_entry_len =  SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U64;
        let serialized_entry = metadata.serialize();

        assert_eq!(serialized_entry.len(), expected_entry_len);
    }
}
