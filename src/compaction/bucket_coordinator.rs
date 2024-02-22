use crate::bloom_filter::{self, BloomFilter};
use crate::memtable::InMemoryTable;
use crate::sstable::SSTable;
use std::collections::HashMap;
use std::path::Path;
use std::{fs, io};
use std::{path::PathBuf, sync::Arc};
use crossbeam_skiplist::SkipMap;
use uuid::Uuid;

const BUCKET_LOW: f64 = 0.5;
const BUCKET_HIGH: f64 = 1.5;
const MIN_SSTABLE_SIZE: usize = 32;
const MIN_TRESHOLD: usize = 4;
const MAX_TRESHOLD: usize = 32;
const  BUCKET_DIRECTORY_PREFIX: &str = "bucket";
#[derive(Debug)]
pub struct BucketMap {
    pub dir: PathBuf,
    pub buckets: HashMap<Uuid, Bucket>,
}
#[derive(Debug)]
pub struct Bucket {
    pub(crate) id: Uuid,
    pub(crate) dir: PathBuf,
    pub(crate) avarage_size: usize,
    pub(crate) sstables: Vec<SSTablePath>,
}
#[derive(Debug, Clone)]
pub struct SSTablePath{
    pub(crate) file_path: String,
    pub(crate) hotness: u64
}
impl  SSTablePath{
    pub fn  new(file_path: String)-> Self{
     Self{
       file_path,
       hotness:0
     }
    }
    pub fn increase_hotness(&mut self){
       self.hotness+=1;
    }
    pub fn get_path(&self)-> String{
        self.file_path.clone()
     }

     pub fn get_hotness(&self)-> u64{
        self.hotness
     }
}

pub trait IndexWithSizeInBytes {
    fn get_index(&self) -> Arc<SkipMap<Vec<u8>, (usize, u64)>>; // usize for value offset, u64 to store entry creation date in milliseconds
    fn size(&self)-> usize;
}

impl Bucket {
    pub fn new(dir: PathBuf) -> Self {
        let bucket_id = Uuid::new_v4();
        let bucket_dir = dir.join(BUCKET_DIRECTORY_PREFIX.to_string() + bucket_id.to_string().as_str()) ;
        fs::create_dir(bucket_dir.clone()).expect("Unable to create file");
        Self {
            id: bucket_id,
            dir: bucket_dir,
            avarage_size: 0,
            sstables: Vec::new(),
        }
    }

    pub fn new_with_id_dir_average_and_sstables( dir: PathBuf, id: Uuid,sstables: Vec<SSTablePath>, mut avarage_size: usize ) -> Bucket{
        if avarage_size==0{
            avarage_size = (sstables.iter().map(|s| fs::metadata(s.get_path()).unwrap().len()).sum::<u64>() / sstables.len() as u64) as usize
        }
        Self {
            id,
            dir,
            avarage_size,
            sstables,
        }
    }
}

impl BucketMap {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            buckets: HashMap::new(),
        }
    }
    pub fn set_buckets(&mut self, buckets: HashMap<Uuid, Bucket>){
     self.buckets = buckets
    }


    pub fn insert_to_appropriate_bucket<T: IndexWithSizeInBytes>(&mut self, table: &T, hotness: u64) -> io::Result<SSTablePath> {
        let added_to_bucket = false;
            
            for (_, bucket) in &mut self.buckets {
                
                // if (bucket low * bucket avg) is less than sstable size 
                if (bucket.avarage_size as f64 * BUCKET_LOW  < table.size() as f64) 
        
                    // and sstable size is less than (bucket avg * bucket high)
                    && (table.size() < (bucket.avarage_size as f64 * BUCKET_HIGH) as usize)
                    
                    // or the (sstable size is less than min sstabke size) and (bucket avg is less than the min sstable size )
                    || ((table.size() as usize) < MIN_SSTABLE_SIZE && bucket.avarage_size  < MIN_SSTABLE_SIZE)
                {
                    
                    let mut sstable = SSTable::new(bucket.dir.clone(), true);
                    sstable.set_index(table.get_index());
                    match sstable.write_to_file() {
                        Ok(_) => {
                            // add sstable to bucket
                            let sstable_path = SSTablePath{
                                file_path: sstable.get_path(),
                                hotness
                            };
                            bucket.sstables.push(sstable_path.clone());
                            bucket.sstables.iter_mut().for_each(|s| s.increase_hotness());
                            bucket.avarage_size = (bucket.sstables.iter().map(|s| fs::metadata(s.get_path()).unwrap().len()).sum::<u64>() / bucket.sstables.len() as u64) as usize;
                            return Ok(sstable_path);
                        }
                        Err(err) => {
                            return Err(io::Error::new(err.kind(), err.to_string()));
                        },
                    }
                }
            }
        
            // create a new bucket if none of the condition above was satisfied
            if !added_to_bucket {
                let mut bucket = Bucket::new(self.dir.clone());
                let mut sstable = SSTable::new(bucket.dir.clone(), true);
                    sstable.set_index(table.get_index());
                    match sstable.write_to_file() {
                        Ok(_) => {
                            // add sstable to bucket
                            let sstable_path = SSTablePath{
                                file_path: sstable.get_path(),
                                hotness:1
                            };
                            bucket.sstables.push(sstable_path.clone());
                            bucket.avarage_size = fs::metadata(sstable.get_path()).unwrap().len() as usize;
                            self.buckets.insert(bucket.id, bucket);
                            
                            return Ok(sstable_path);
                        }
                        Err(err) => {
                            return Err(io::Error::new(err.kind(), err.to_string()));
                        },
                    }
            }
            
            Err(io::Error::new(io::ErrorKind::Other, "No condition for insertion was stisfied"))
        }



        pub fn extract_buckets_to_compact(&self) -> (Vec<Bucket>, Vec<(Uuid, Vec<SSTablePath>)>) {

            let mut sstables_to_delete: Vec<(Uuid, Vec<SSTablePath>)> = Vec::new();
            let mut buckets_to_compact: Vec<Bucket> = Vec::new();
            // Extract buckets
            self.buckets
                .iter()
                .enumerate()
                .filter(|elem| {
                    // b for bucket :)
                    let b = elem.1 .1;
                    b.sstables.len() >= MIN_TRESHOLD
                })
                .for_each(|(_, elem)| {
                    let b = elem.1;
                    // get the sstables we have to delete after compaction is complete
                    // if the number of sstables is more than the max treshhold then just extract the first 32 sstables
                    // otherwise extract all the sstables in the bucket
                    sstables_to_delete.push((b.id, b.sstables.get(0..MAX_TRESHOLD).unwrap_or(&b.sstables).to_vec()));
                
                    buckets_to_compact.push(Bucket {
                        sstables: b.sstables.get(0..MAX_TRESHOLD).unwrap_or(&b.sstables).to_vec(),
                        id: b.id,
                        dir: b.dir.to_owned(),
                        avarage_size: b.avarage_size, // passing the average size is redundant here becuase
                                                      // we don't need it for the actual compaction but we leave it to keep things readable
                    })
                });
            (buckets_to_compact, sstables_to_delete)
        }


        // NOTE:  This should be called only after compaction is complete

        pub fn delete_sstables(&mut self, sstables_to_delete: &Vec<(Uuid, Vec<SSTablePath>)>) -> bool {
            let mut all_sstables_deleted = true;
            let mut buckets_to_delete: Vec<&Uuid> = Vec::new();
        
            for (bucket_id, sst_paths) in sstables_to_delete {
                if let Some(bucket) = self.buckets.get_mut(bucket_id) {
                    let sstables_remaining = bucket.sstables.get(sst_paths.len()..).clone().unwrap_or_default();
        
                    if !sstables_remaining.is_empty() {
                        *bucket = Bucket {
                            id: bucket.id,
                            dir: bucket.dir.clone(),
                            avarage_size: bucket.avarage_size,
                            sstables: sstables_remaining.to_vec(),
                        };
                    } else {
                        buckets_to_delete.push(bucket_id);
        
                        if let Err(err) = fs::remove_dir_all(&bucket.dir) {
                            eprintln!("Error removing directory: {}", err);
                        } else {
                            println!("Bucket successfully removed with bucket id {}", bucket_id);
                        }
                    }
                }
        
                for sst in sst_paths {
                    if SSTable::file_exists(&PathBuf::new().join(sst.get_path())) {
                         //bloom_filters_map.remove(&sst.get_path());
                        if let Err(err) = fs::remove_file(&sst.file_path) {
                            all_sstables_deleted = false;
                            eprintln!("Error deleting SS Table file: {}", err);
                        } else {
                            println!("SS Table deleted successfully.");
                        }
                    }
                }
            }
        
            if !buckets_to_delete.is_empty() {
                buckets_to_delete.iter().for_each(|&bucket_id| {
                    self.buckets.remove(bucket_id);
                });
            }
        
            // bloom_filters.clear();
            // bloom_filters.extend(bloom_filters_map.into_iter().map(|(_, bf)| bf));
        
            // println!("SS TABLES DELETED: {:?}", sstables_to_delete.iter().map(|e| e.1.len()).sum::<usize>());
            // println!("Old Bloom Filters: {:?}", bloom_filters.len());
            // println!("=======================================");
            // println!("Updated Bloom Filters: {:?}", bloom_filters.len());
        
            all_sstables_deleted
        }
        
}
