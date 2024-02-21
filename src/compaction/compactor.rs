use std::{cmp::Ordering, io, mem, path::PathBuf, sync::Arc};

use crossbeam_skiplist::SkipMap;
use uuid::Uuid;

use crate::{
    bloom_filter::{self, BloomFilter},
    memtable::{Entry, DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MEMTABLE_CAPACITY},
    sstable::{self, SSTable},
    storage_engine::SizeUnit,
};

use super::{bucket_coordinator::Bucket, BucketMap, SSTablePath};

pub struct Compactor;
pub(crate) struct MergedSSTable {
    sstable: SSTable,
    hotness: u64,
    bloom_filter: BloomFilter,
}

impl MergedSSTable {
    pub fn new(sstable: SSTable, bloom_filter: BloomFilter, hotness: u64) -> Self {
        Self {
            sstable,
            hotness,
            bloom_filter,
        }
    }
}

impl Compactor {
    pub fn new() -> Self {
        return Self;
    }

    pub fn run_compaction(&self, buckets: &mut BucketMap, bloom_filters: &mut Vec<BloomFilter>) -> io::Result<bool> {
        let mut number_of_compactions =0;
        // The compaction loop will keep running until there 
        // are no more buckets with more than minimum treshold size

        // TODO: See if we can handle this with multiple threads while keeping track of number of Disk IO used 
        // so that we don't run out of Disk IO during large compactions
        loop {
        // Step 1: Extract buckets to compact
        let buckets_to_compact_and_sstables_to_remove = buckets.extract_buckets_to_compact();
        let buckets_to_compact =  buckets_to_compact_and_sstables_to_remove.0;
        let sstables_files_to_remove = buckets_to_compact_and_sstables_to_remove.1;
        
        // Exit the compaction loop if there are no more buckets to compact
        if buckets_to_compact.is_empty(){
            println!("{} ALL COMPACTION COMPLETED", number_of_compactions);
            return Ok(true)
        }
        number_of_compactions+=1;
        // Step 2: Merge SSTables in each buckct
        let merged_sstable_opt = self.merge_sstables_in_buckets(&buckets_to_compact);
        let mut actual_number_of_sstables_written_to_disk = 0;
        let mut expected_sstables_to_be_writtten_to_disk = 0;
        match merged_sstable_opt {
            Some(merged_sstables) => {
                // Number of sstables expected to be inserted to disk
                expected_sstables_to_be_writtten_to_disk = merged_sstables.len();

                //Step 3: Write merged sstables to bucket map
                merged_sstables
                    .into_iter()
                    .enumerate()
                    .for_each(|(_, mut m)| {
                        let insert_result =
                            buckets.insert_to_appropriate_bucket(&m.sstable, m.hotness);
                        match insert_result {
                            Ok(sst_file_path) => {
                                // Step 4: Map this bloom filter to its sstable file path
                                m.bloom_filter.set_sstable_path(sst_file_path);
                                // Step 5: Store the bloom filter in the bloom filters vector
                                bloom_filters.push(m.bloom_filter);
                                actual_number_of_sstables_written_to_disk += 1;
                            }
                            Err(_) =>  {
                                println!(
                                    "merged SSTable was not written to disk "
                                )
                            },
                        }
                    })
            }
            None => {}
        }

        println!(
        "Expected number of new SSTables written to disk : {}, Actual number of SSTables written {}",
         expected_sstables_to_be_writtten_to_disk, 
         actual_number_of_sstables_written_to_disk 
        );

        if expected_sstables_to_be_writtten_to_disk == actual_number_of_sstables_written_to_disk{
            
            // Step 6:  Delete the sstables that we already merged from their previous buckets
            let updated_bloom_filters_opt = self.clean_up_after_compaction(buckets, &sstables_files_to_remove, bloom_filters);
            match updated_bloom_filters_opt {
                Some(updated_bloom_filters)=>{
                    bloom_filters.clear();
                    // update bloom filter vector
                    bloom_filters.extend_from_slice(&updated_bloom_filters.clone());
                    bloom_filters.iter().for_each(|bf|{
                        println!("THIS IS OUT NEW BF {:?}", bf.sstable_path)
                    });
                   
                }
                None=> {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "Bloom Filter was not updated successfully"));
                }
            }
        }
          println!("{} COMPACTION COMPLETED SUCCESSFULLY", number_of_compactions);

        }
    
    }

    pub fn clean_up_after_compaction(&self,  buckets: &mut BucketMap,  sstables_to_delete: &Vec<(Uuid, Vec<SSTablePath>)>, bloom_filters_with_both_old_and_new_sstables: &mut Vec<BloomFilter>)-> Option<Vec<BloomFilter>>{
       let all_sstables_deleted = buckets.delete_sstables(&sstables_to_delete);
       
       // if all sstables were not deleted then don't remove the associated bloom filters
       // although this can lead to redundancy bloom filters are in-memory and its also less costly 
       // since keys are represented in bits  
       if all_sstables_deleted{
        // Step 7: Delete the bloom filters associated with the sstables that we already merged
        let updated_bloom_filters  = self.filter_out_old_bloom_filters(bloom_filters_with_both_old_and_new_sstables, sstables_to_delete);
         return Some(updated_bloom_filters);
       }
       None
    }
    
    pub fn filter_out_old_bloom_filters(&self, bloom_filters_with_both_old_and_new_sstables: &mut Vec<BloomFilter>, sstables_to_delete: &Vec<(Uuid, Vec<SSTablePath>)>)-> Vec<BloomFilter>{
        println!("How far have you gone before {}", bloom_filters_with_both_old_and_new_sstables.len());
        let mut new_bf = Vec::new();
        println!("SS tabels to delete {:?}", sstables_to_delete.clone());
        bloom_filters_with_both_old_and_new_sstables
            .into_iter()
            .for_each(|b| {
                let mut bf_sst_path_not_found_in_sstables_to_delete = true;
                sstables_to_delete.iter().for_each(
                    |(_, sstable_files_paths)| {
                        sstable_files_paths.iter().for_each(
                            |file_path_to_delete| {
                                if b.sstable_path.as_ref()
                                    .unwrap()
                                    .file_path
                                    == file_path_to_delete.file_path{
                                        bf_sst_path_not_found_in_sstables_to_delete= false
                                    }
                               
                            },
                        )
                    },
                );
                if bf_sst_path_not_found_in_sstables_to_delete{
                    new_bf.push(b.to_owned());
                }
            });
    println!("How far have you gone {}", new_bf.len());
       new_bf
    }


    fn merge_sstables_in_buckets(&self, buckets: &Vec<Bucket>) -> Option<Vec<MergedSSTable>> {
        let mut merged_sstbales: Vec<MergedSSTable> = Vec::new();

        buckets.iter().for_each(|b| {
            let mut hotness = 0;
            let sstable_paths = &b.sstables;
            
            let mut merged_sstable= SSTable::new(b.dir.clone(), false);
            sstable_paths.iter().for_each(|path| {
                hotness += path.hotness;
                let sst_opt = SSTable::from_file(PathBuf::new().join(path.get_path())).unwrap();
                match sst_opt {
                    Some(sst) => {
                        merged_sstable = self.merge_sstables(&merged_sstable, &sst);
                    }
                    None => {}
                }
            });

            // Rebuild the bloom filter since a new sstable has been created
            let new_bloom_filter = self.build_bloomfilter_from_sstable(&merged_sstable.index);
            merged_sstbales.push(MergedSSTable {
                sstable: merged_sstable,
                hotness,
                bloom_filter: new_bloom_filter,
            })
        });
        if merged_sstbales.len() == 0 {
            return None;
        }
        Some(merged_sstbales)
    }

    fn build_bloomfilter_from_sstable(
        &self,
        index: &Arc<SkipMap<Vec<u8>, (usize, u64)>>,
    ) -> BloomFilter {
        // Rebuild the bloom filter since a new sstable has been created
        let mut new_bloom_filter = BloomFilter::new(DEFAULT_FALSE_POSITIVE_RATE, index.len());
        index.iter().for_each(|e| new_bloom_filter.set(e.key()));
        new_bloom_filter
    }

    fn merge_sstables(&self, sst1: &SSTable, sst2: &SSTable) -> SSTable {
        let mut new_sstable = SSTable::new(PathBuf::new(), false);
        let new_sstable_index = Arc::new(SkipMap::new());
        let mut merged_indexes = Vec::new();
        let index1 = sst1
            .get_index()
            .iter()
            .map(|e| Entry::new(e.key().to_vec(), e.value().0, e.value().1))
            .collect::<Vec<Entry<Vec<u8>, usize>>>();

        let index2 = sst2
            .get_index()
            .iter()
            .map(|e| Entry::new(e.key().to_vec(), e.value().0, e.value().1))
            .collect::<Vec<Entry<Vec<u8>, usize>>>();
        
        let (mut i, mut j) = (0, 0);
        // Compare elements from both arrays and merge them
        while i < index1.len() && j < index2.len() {

            match index1[i].key.cmp(&index2[j].key)  {
                Ordering::Less => {
                    // increase new_sstable size
                    merged_indexes.push(index1[i].clone());
                    i += 1;
                },
                Ordering::Equal =>{
                    if index1[i].created_at > index2[j].created_at {
                        merged_indexes.push(index1[i].clone());
                    } else {
                        merged_indexes.push(index2[j].clone());
                    }
                    i += 1;
                    j += 1;
                },
                Ordering::Greater =>{
                    merged_indexes.push(index2[j].clone());
                    j += 1;
                }
            }
        }

        // If there are any remaining elements in arr1, append them
        while i < index1.len() {
            merged_indexes.push(index1[i].clone());
            i += 1;
        }

        // If there are any remaining elements in arr2, append them
        while j < index2.len() {
            merged_indexes.push(index2[j].clone());
            j += 1;
        }
        merged_indexes.iter().for_each(|e| {
            new_sstable_index.insert(e.key.to_owned(), (e.val_offset, e.created_at));
        });
        new_sstable.set_index(new_sstable_index);
        new_sstable
    }
}
