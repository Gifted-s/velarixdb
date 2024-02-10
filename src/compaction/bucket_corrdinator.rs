use crate::memtable::InMemoryTable;
use crate::sstable::SSTable;
use std::collections::HashMap;
use std::{fs, io};
use std::{path::PathBuf, sync::Arc};
use uuid::Uuid;

const BUCKET_LOW: f64 = 0.5;
const BUCKET_HIGH: f64 = 1.5;
const MIN_SSTABLE_SIZE: usize = 32;
const MIN_TRESHOLD: usize = 4;
const MAX_TRESHOLD: usize = 32;
#[derive(Debug)]
pub struct BucketMap {
    dir: PathBuf,
    buckets: HashMap<Uuid, Bucket>,
}
#[derive(Debug)]
pub struct Bucket {
    id: Uuid,
    dir: PathBuf,
    avarage_size: usize,
    sstables: Vec<SSTablePath>, // string represents the file path to this sstable
}
#[derive(Debug, Clone)]
pub struct SSTablePath{
    file_path: String,
    hotness: u64
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
}

impl Bucket {
    pub fn new(dir: PathBuf) -> Self {
        let bucket_id = Uuid::new_v4();
        let bucket_dir = dir.join(String::from("bucket") + bucket_id.to_string().as_str()) ;
        fs::create_dir(bucket_dir.clone()).expect("Unable to create file");
        Self {
            id: bucket_id,
            dir: bucket_dir,
            avarage_size: 0,
            sstables: Vec::new(),
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


    pub fn insert_to_appropriate_bucket(&mut self, memtable: &InMemoryTable<Vec<u8>>) -> io::Result<SSTablePath> {
        let added_to_bucket = false;
            
            for (_, bucket) in &mut self.buckets {
                
                // if (bucket low * bucket avg) is less than sstable size 
                if (bucket.avarage_size as f64 * BUCKET_LOW  < memtable.size as f64) 
        
                    // and sstable size is less than (bucket avg * bucket high)
                    && (memtable.size < (bucket.avarage_size as f64 * BUCKET_HIGH) as usize)
                    
                    // or the (sstable size is less than min sstabke size) and (bucket avg is less than the min sstable size )
                    || ((memtable.size as usize) < MIN_SSTABLE_SIZE && bucket.avarage_size  < MIN_SSTABLE_SIZE)
                {
                    
                    let mut sstable = SSTable::new(bucket.dir.clone(), true);
                    sstable.set_index(memtable.index.clone());
                    match sstable.write_to_file() {
                        Ok(_) => {
                            // add sstable to bucket
                            let sstable_path = SSTablePath{
                                file_path: sstable.get_path(),
                                hotness:1
                            };
                            bucket.sstables.push(sstable_path.clone());
                            bucket.sstables.iter_mut().for_each(|s| s.increase_hotness());
                            bucket.avarage_size = (bucket.sstables.iter().map(|s| fs::metadata(s.get_path()).unwrap().len()).sum::<u64>() / bucket.sstables.len() as u64) as usize;
                            // for test
                            self.buckets.iter().for_each(|b|{
                                b.1.sstables.iter().for_each(|s|{
                                    println!("Inserted to bucket {:?} \n", s);
                                });
                            });
                           
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
                    sstable.set_index(memtable.index.clone());
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
           //TODO: Sort the buckets by hotness
           //self.buckets.sort_by(|a,b| b.iter().map(|s| s.hotness).sum::<u64>().cmp(&a.iter().map(|s| s.hotness).sum::<u64>()));
        
        }
    
}
