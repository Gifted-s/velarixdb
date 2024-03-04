extern crate nix;
extern crate libc;

use std::fs::File;
use std::io::{Error, Result};
use std::os::unix::io::AsRawFd;

use nix::libc::{c_int, off_t};

extern "C" {
    fn fallocate(fd: libc::c_int, mode: c_int, offset: off_t, len: off_t) -> c_int;
}

const FALLOC_FL_PUNCH_HOLE: c_int = 0x2;
const FALLOC_FL_KEEP_SIZE: c_int = 0x1;

fn punch_holes(file_path: &str, offset: off_t, length: off_t) -> Result<()> {
    let file = File::open(file_path)?;

    let fd = file.as_raw_fd();

    unsafe {
        let result = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, length);

        if result == 0 {
            Ok(())
        } else {
            Err(Error::last_os_error())
        }
    }
}

fn main() {
    let file_path = "example.txt";
    let offset = 10; // Offset where holes should start
    let length = 120; // Length of the hole to punch

    match punch_holes(file_path, offset, length) {
        Ok(()) => println!("Holes punched successfully"),
        Err(err) => eprintln!("Error punching holes: {}", err),
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::bloom_filter;
    use log::info;
    use std::fs::remove_dir;

    // Generate test to find keys after compaction
    #[test]
    fn gc_test() {
       // Specify the file path, offset, and length for punching a hole
    let file_path = "example.txt";
    let offset = 10;
    let length = 120;

    // Call the punch_hole function
    match punch_holes(file_path, offset, length) {
        Ok(()) => println!("Hole punched successfully."),
        Err(err) => eprintln!("Error punching hole: {}", err),
    }
    }
}
