use crate::{bloom_filter::BloomFilter, sstable::SSTable};

use super::{bucket_coordinator::Bucket, BucketMap};

struct compactor;

impl compactor {
    pub fn new()->Self{
        return Self
    }

    fn run_compaction(&self, buckets: &mut BucketMap, bloom_filters: &mut Vec<BloomFilter>){
      // Step 1: Extract buckets to compact
       let buckets_to_compact = buckets.extract_buckets_to_compact();

       // Step 2: Merge SSTables in each buckct

    }



    fn merge_sstables_in_buckets(&self, buckets: &Vec<Bucket>) -> Vec<(SSTable, BloomFilter)>{
        let  mut sstables_and_their_associated_bloom_filter: Vec<(SSTable, BloomFilter)> = Vec::new();
        
        buckets.iter().for_each(|b|{
            let ssttable_paths = b.sstables;
        })
        
    }
}
