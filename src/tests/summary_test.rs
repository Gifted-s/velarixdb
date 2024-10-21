#[cfg(test)]
mod tests {
    use crate::consts::{SIZE_OF_U32, SUMMARY_FILE_NAME};
    use crate::sst::Summary;
    use crate::tests::workload::SSTContructor;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_summary_new() {
        let root = tempdir().unwrap();
        let path = root.path().join("summary_new");

        let summary = Summary::new(path.to_owned());

        assert_eq!(summary.smallest_key, vec![]);
        assert_eq!(summary.biggest_key, vec![]);
        assert_eq!(summary.path, path.join(format!("{}.db", SUMMARY_FILE_NAME)));
    }

    #[tokio::test]
    async fn test_summary_recover() {
        let sst = SSTContructor::generate_ssts(1).await[0].to_owned();

        let mut recovered_summary = Summary::new(sst.dir);
        let res = recovered_summary.recover().await;
        assert!(res.is_ok());
        assert!(!recovered_summary.biggest_key.is_empty());
        assert!(!recovered_summary.smallest_key.is_empty());
    }

    #[tokio::test]
    async fn test_summary_write() {
        let sst = SSTContructor::generate_ssts(1).await[0].to_owned();

        let mut recovered_summary = Summary::new(sst.dir.to_owned());
        let res = recovered_summary.recover().await;
        assert!(res.is_ok());
        assert!(recovered_summary.write_to_file().await.is_ok())
    }

    #[tokio::test]
    async fn test_summary_serialize() {
        let root = tempdir().unwrap();
        let path = root.path().join("summary_write");

        let mut summary = Summary::new(path);
        summary.biggest_key = vec![1, 2, 3];
        summary.smallest_key = vec![0, 2, 3];

        let expected_entry_len =
            SIZE_OF_U32 + SIZE_OF_U32 + summary.biggest_key.len() + summary.smallest_key.len();
        let serialized_entry = summary.serialize();

        assert_eq!(serialized_entry.len(), expected_entry_len);
    }
}
