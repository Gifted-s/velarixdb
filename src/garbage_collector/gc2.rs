// extern crate libc;

// use std::fs::File;
// use std::io::{Error, Result};
// use std::os::unix::io::AsRawFd;

// #[repr(C)]
// struct fstore {
//     fst_flags: libc::c_uint,
//     fst_posmode: libc::c_int,
//     fst_offset: libc::off_t,
//     fst_length: libc::off_t,
//     fst_bytesalloc: libc::off_t,
// }

// extern "C" {
//     fn fstore(fd: libc::c_int, cmd: libc::c_uint, fstore: *mut fstore) -> libc::c_int;
// }

// const F_ALLOCATECONTIG: libc::c_uint = 0x00000002;
// const F_ALLOCATEALL: libc::c_uint = 0x00000004;
// const F_PUNCHHOLE: libc::c_uint = 0x00000008;

// fn punch_holes(file_path: &str, offset: libc::off_t, length: libc::off_t) -> Result<()> {
//     let file = File::open(file_path)?;

//     let fd = file.as_raw_fd();

//     let mut fstore_info = fstore {
//         fst_flags: F_PUNCHHOLE,
//         fst_posmode: 0,
//         fst_offset: offset,
//         fst_length: length,
//         fst_bytesalloc: 0,
//     };

//     unsafe {
//         let result = fstore(fd, 0, &mut fstore_info);

//         if result == 0 {
//             Ok(())
//         } else {
//             Err(Error::last_os_error())
//         }
//     }
// }

// fn main() {
//     let file_path = "example.txt";
//     let offset = 1024; // Offset where holes should start
//     let length = 4096; // Length of the hole to punch

//     match punch_holes(file_path, offset, length) {
//         Ok(()) => println!("Holes punched successfully"),
//         Err(err) => eprintln!("Error punching holes: {}", err),
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::bloom_filter;
//     use log::info;
//     use std::fs::remove_dir;

//     // Generate test to find keys after compaction
//     #[test]
//     fn gc_test() {
//        // Specify the file path, offset, and length for punching a hole
//     let file_path = "example.txt";
//     let offset = 10;
//     let length = 120;

//     // Call the punch_hole function
//     match punch_holes(file_path, offset, length) {
//         Ok(()) => println!("Hole punched successfully."),
//         Err(err) => eprintln!("Error punching hole: {}", err),
//     }
//     }
// }
