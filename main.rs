
// fn main() {
//     #[derive(Clone, Copy, PartialEq, Debug)]
//     struct SSTable {
//         size: u64,
//         hotness: u64,
//     }
//     // SSTables before compaction starts
//     let sstables = vec![
//         SSTable {
//             size: 78,
//             hotness: 0,
//         },
//         SSTable {
//             size: 51,
//             hotness: 0,
//         },
//         SSTable {
//             size: 100,
//             hotness: 0,
//         },
//         SSTable {
//             size: 60,
//             hotness: 0,
//         },
//         SSTable {
//             size: 19,
//             hotness: 0,
//         },
//         SSTable {
//             size: 27,
//             hotness: 0,
//         },
//         SSTable {
//             size: 34,
//             hotness: 0,
//         },
//         SSTable {
//             size: 7,
//             hotness: 0,
//         },
//         SSTable {
//             size: 1,
//             hotness: 0,
//         },
//         SSTable {
//             size: 10,
//             hotness: 0,
//         },
//     ];
//     // Sorting SSTables by size in descending order
//     let mut sorted_sstables = sstables.clone();
//     sorted_sstables.sort_by(|a, b| b.size.cmp(&a.size));
    
//     let BUCKET_LOW = 0.5;
//     let BUCKET_HIGH = 1.5;
//     let MIN_SSTABLE_SIZE = 32;
//     let min_threshhold = 4;
//     let max_threshold = 32;

//     // Bucketing SSTables
//     let mut buckets: Vec<Vec<SSTable>> = Vec::new();
     
//     // loop through the  sorted_sstables
//     for sstable in &sorted_sstables {
//         let mut added_to_bucket = false;
        
//         for bucket in &mut buckets {
//             // calculate the sstables average size
//             let bucket_avg_size: u64 =
//                 bucket.iter().map(|s| s.size).sum::<u64>() / bucket.len() as u64;
            
//             // if (bucket low * bucket avg) is less than sstable size 
//             if (bucket_avg_size as f64 * BUCKET_LOW  < sstable.size as f64) 

//                 // and sstable size is less than (bucket avg * bucket high)
//                 && (sstable.size < (bucket_avg_size as f64 * BUCKET_HIGH) as u64)
                
//                 // or the (sstable size is less than min sstabke size) and (bucket avg is less than the min sstable size )
//                 || (sstable.size < MIN_SSTABLE_SIZE && bucket_avg_size < MIN_SSTABLE_SIZE)
//             {
//                 bucket.push(*sstable);
//                 added_to_bucket = true;

//                 bucket.iter_mut().for_each(|s| s.hotness += 1);
//                 break;
//             }
//         }

//         // create a new bucket if none of the condition above was satisfied
//         if !added_to_bucket {
//             let new_bucket = vec![*sstable];
//             buckets.push(new_bucket);
//         }

//     }

//     buckets.sort_by(|a,b| b.iter().map(|s| s.hotness).sum::<u64>().cmp(&a.iter().map(|s| s.hotness).sum::<u64>()));


//     //NOTE: the following happen during compaction
//     let mut remainder_sstables: Vec<Vec<SSTable>> = vec![];
//     let mut buckets_to_compact: Vec<Vec<SSTable>> = buckets.into_iter().enumerate().filter(|b| b.1.len()>=4).map(|(_, mut bucket)|{
//     if bucket.len() > max_threshold{
//         remainder_sstables.push(bucket[(max_threshold+1)..].to_vec());
//         bucket.truncate(max_threshold);
//       }
//       bucket
//     }).collect();
    
//     println!("Buckets to compact {:?} \n", buckets_to_compact);
//     println!("Buckets remainder {:?} \n", remainder_sstables);
    
// }
    // let mut compacted_sstables: Vec<SSTable> = selected_buckets.into_iter().flat_map(|mut bucket| {
    //     bucket.sort_by(|a, b| b.hotness.cmp(&a.hotness));
    //     bucket.into_iter().take(max_threshold)
    // }).collect();

    // for s in &mut compacted_sstables {
    //     println!("{:?} \n", s);
    // }

    const BUCKET_LOW: f64 = 0.5;
    const  BUCKET_HIGH:f64 = 1.5;
    const MIN_SSTABLE_SIZE: usize = 32;
    const MIN_TRESHOLD: usize = 4;
    const  MAX_TRESHOLD: usize = 32;
    
    
    #[derive(Clone, PartialEq, Debug)]
    struct SStable {
    size: u64,
    keys: Vec<Vec<u8>>, // make this a vector of keys
    hotness: u64,
    }
    
    
    fn main() {
    // SSTables before compaction starts
    let sstables = vec![
        SStable {
            keys: vec!["ayomide".as_bytes().to_vec(), "sunkanmi".as_bytes().to_vec()],
            size: 78,
            hotness: 0,
        },
        SStable {
            keys: vec!["ayodeji".as_bytes().to_vec(), "sunkanmi".as_bytes().to_vec()],
            size: 51,
            hotness: 0,
        },
        SStable {
            keys: vec!["ayodeji".as_bytes().to_vec(), "bayo".as_bytes().to_vec()],
            size: 100,
            hotness: 0,
        },
        SStable {
            keys: vec!["ayodeji".as_bytes().to_vec(), "bayo".as_bytes().to_vec()],
            size: 60,
            hotness: 0,
        },
        SStable {
            keys: vec!["bayo".as_bytes().to_vec(), "kolade".as_bytes().to_vec()],
            size: 19,
            hotness: 0,
        },
        SStable {
            keys: vec!["ayodeji".as_bytes().to_vec(), "sunkanmi".as_bytes().to_vec()],
            size: 27,
            hotness: 0,
        },
        SStable {
            keys: vec!["ayodeji".as_bytes().to_vec(), "kunle".as_bytes().to_vec()],
            size: 34,
            hotness: 0,
        },
        SStable {
            keys: vec!["ayodeji".as_bytes().to_vec(), "kayode".as_bytes().to_vec()],
            size: 7,
            hotness: 0,
        },
        // SStable {
        //     keys: vec!["yinka".into(),"bukola".into()],
        //     size: 1,
        //     hotness: 0,
        // },
        // SStable {
        //     keys: vec!["bukola".into(), "ayodeji".into()],
        //     size: 10,
        //     hotness: 0,
        // },
    ];
    
    
    
    // Sorting SSTables by size in descending order
    let mut sorted_sstables = sstables.clone();
    sorted_sstables.sort_by(|a, b| b.size.cmp(&a.size));
    
    
    
    // Bucketing SSTables
    let mut buckets: Vec<Vec<SStable>> = Vec::new();
     
    // loop through the  sorted_sstables
    for sstable in &sorted_sstables {
       insert_to_appropriate_bucket(&mut buckets, sstable)
    }
    
    let (buckets_to_compact, buckets_not_reaching_treshold) = extract_buckets_to_compact(buckets);
    
    let key_sum:u64 = buckets_to_compact.iter().map(|b| b.iter().map(|s| s.keys.len() as u64).sum::<u64>()).sum();
    println!("{:?} No of keys to compact", key_sum);

    let compacted_sstables = compact(&buckets_to_compact);
    let mut new_buckets: Vec<Vec<SStable>> = Vec::new();

    // We insert the buckets not reaching the treshold first because it is possible that
    // one of the compacted sstable might fall into them 
    new_buckets.extend(buckets_not_reaching_treshold);
    compacted_sstables.iter().for_each(|s|{
        insert_to_appropriate_bucket(&mut new_buckets, s);
    });
    
    let key_sum:u64 = compacted_sstables.iter().map(|s|  s.keys.len() as u64).sum::<u64>();
    println!("{:?} No of keys afer compaction", key_sum);
    }
    
    fn insert_to_appropriate_bucket(buckets: &mut Vec<Vec<SStable>>, sstable: &SStable){
    let mut added_to_bucket = false;
        
        for bucket in &mut *buckets {
            // calculate the sstables average size
            let bucket_avg_size: usize =
                (bucket.iter().map(|s| s.size).sum::<u64>() / bucket.len() as u64) as usize;
            
            // if (bucket low * bucket avg) is less than sstable size 
            if (bucket_avg_size as f64 * BUCKET_LOW  < sstable.size as f64) 
    
                // and sstable size is less than (bucket avg * bucket high)
                && (sstable.size < (bucket_avg_size as f64 * BUCKET_HIGH) as u64)
                
                // or the (sstable size is less than min sstabke size) and (bucket avg is less than the min sstable size )
                || ((sstable.size as usize) < MIN_SSTABLE_SIZE && bucket_avg_size  < MIN_SSTABLE_SIZE)
            {
                bucket.push(sstable.clone());
                added_to_bucket = true;
    
                bucket.iter_mut().for_each(|s| s.hotness += 1);
                break;
            }
        }
    
        // create a new bucket if none of the condition above was satisfied
        if !added_to_bucket {
            let new_bucket = vec![sstable.clone()];
            buckets.push(new_bucket);
        }
    
    buckets.sort_by(|a,b| b.iter().map(|s| s.hotness).sum::<u64>().cmp(&a.iter().map(|s| s.hotness).sum::<u64>()));
    
    }
    
    
    
    
    fn extract_buckets_to_compact(buckets: Vec<Vec<SStable>>) -> (Vec<Vec<SStable>>, Vec<Vec<SStable>>){
    
     // this will hold buckets that contains remainder sstables after the maximum sstables has been selected for compaction
     let mut buckets_with_remainder: Vec<Vec<SStable>> = vec![];
    
     // this will hold the buckets that does not reach the treshold
     let mut buckets_not_reaching_treshold: Vec<Vec<SStable>> = vec![];
    
     // this contains the bucket that we want to compact
     let  buckets_to_compact: Vec<Vec<SStable>> = buckets.into_iter().enumerate().filter(|b| {
         let up_to_min_treshold = b.1.len()>=MIN_TRESHOLD;
         if !up_to_min_treshold{
          buckets_not_reaching_treshold.push(b.1.clone())
         }
         up_to_min_treshold
     }).map(|(_, mut bucket)|{
     if bucket.len() > MAX_TRESHOLD{
         buckets_with_remainder.push(bucket[(MAX_TRESHOLD+1)..].to_vec());
         bucket.truncate(MAX_TRESHOLD);
       }
       bucket
     }).collect();
    
     buckets_not_reaching_treshold.extend(buckets_with_remainder);
     buckets_not_reaching_treshold.sort_by(|a,b| b.iter().map(|s| s.hotness).sum::<u64>().cmp(&a.iter().map(|s| s.hotness).sum::<u64>()));
     
     (buckets_to_compact, buckets_not_reaching_treshold)
    
    }
    
    
    fn compact(buckets_to_compact: &Vec<Vec<SStable>>)-> Vec<SStable>{
     let mut result: Vec<SStable> = Vec::new();
     for bucket in buckets_to_compact{
        let mut merged_keys:Vec<Vec<u8>>= Vec::new();
        let mut hotness_sum =0;
        let mut size_sum =0;
        for sstable in &bucket[1..] {
            hotness_sum+=sstable.hotness;
            size_sum+=sstable.size;
            merged_keys = merge_sstables(&merged_keys, &sstable.keys);
        }
        result.push(SStable{
          keys: merged_keys,
          hotness:hotness_sum,
          size: size_sum
        });
    }
    // Convert back to string for printing
    // let result_str: Vec<String> = merged_keys.iter().map(|v| String::from_utf8_lossy(&v).to_string()).collect();
    // println!("{:?}", result_str);
    result
}


fn merge_sstables(arr1: &Vec<Vec<u8>>, arr2: &Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    let mut merged_array = Vec::new();
    let (mut i, mut j) = (0, 0);

    // Compare elements from both arrays and merge them
    while i < arr1.len() && j < arr2.len() {
        if arr1[i][0] < arr2[j][0] {
            merged_array.push(arr1[i].clone());
            i += 1;
        } 
        else if  arr1[i][0] == arr2[j][0] {
            merged_array.push(arr1[i].clone());
            i+=1;
            j+=1;
        } 
        else {
            merged_array.push(arr2[j].clone());
            j += 1;
        }
    }

    // If there are any remaining elements in arr1, append them
    while i < arr1.len() {
        merged_array.push(arr1[i].clone());
        i += 1;
    }

    // If there are any remaining elements in arr2, append them
    while j < arr2.len() {
        merged_array.push(arr2[j].clone());
        j += 1;
    }

    merged_array
}



