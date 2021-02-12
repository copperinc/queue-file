

#[cfg(test)]
mod tests {
	use std::path::Path;

	use crate::QueueFile;
	use crate::tests::{
		gen_rand_file_name,
		simulate_use,
	};

	// Make a fixed size file as a standin for a raw block device
	fn make_fixed_size_file(fname: &str, len: u64) -> Result<(), std::io::Error> {
	    let fraw = std::fs::File::create(fname)?;
	    fraw.set_len(len)?;
	    fraw.sync_all()?;	
	    Ok(())
	}

	fn run_w_fixedfile<F: Fn()>(fname: &str, len: u64, f: F) {
		make_fixed_size_file(fname, len).expect("create rawfile");

		f();

		std::fs::remove_file(fname).expect("file cleanup");	
	}


	#[test]
	fn rawblockdev_basic() {
		const FNAME: &str = "qf-v2-raw1.img";
		const FILE_LEN: u64 = 80;

		// a raw block device expects a dev of that size in existence already
		run_w_fixedfile(FNAME, FILE_LEN, || {
			let mut qf = QueueFile::create(FNAME, Some(FILE_LEN)).expect("create fail");

			assert!(qf.rawblockdev());

			let val: u32 = 42;

			qf.add(&val.to_be_bytes()).expect("add fail");
			assert_eq!(qf.is_empty(), false);

			let val2: u32 = 100128;
			qf.add(&val2.to_be_bytes()).expect("add fail");

			let vpeek = qf.peek().expect("peek fail").expect("value");
			assert_eq!(&vpeek[..], &val.to_be_bytes());

			qf.remove().expect("remove fail");

			qf.clear().unwrap_or_else(|_| {
				dbg!(&qf);
				assert!(false, "clear fail");
			});

			qf.add(&val2.to_be_bytes()).expect("add fail");
			qf.clear().unwrap_or_else(|_| {
				dbg!(&qf);
				assert!(false, "clear fail");
			});
			qf.add(&val.to_be_bytes()).expect("add fail");

			qf.add(&val2.to_be_bytes()).expect("add fail");
			qf.remove().expect("remove fail");
			qf.add(&val2.to_be_bytes()).expect("add fail");
			qf.remove().expect("remove fail");
			qf.add(&val2.to_be_bytes()).expect("add fail");
			qf.remove().expect("remove fail");

			qf.clear().unwrap_or_else(|_| {
				dbg!(&qf);
				assert!(false, "clear fail");
			});
		});
	}


	#[test]
	fn rawblockdev_wraparound() {
		const FNAME: &str = "qf-v2-raw2.img";
		const FILE_LEN: u64 = 80;

		use crate::tests::gen_rand_data;
		// the V2 format as a 36 byte qf header, 8 byte (len, crc) element header

		run_w_fixedfile(FNAME, FILE_LEN, || {
			let mut qf = QueueFile::create(FNAME, Some(FILE_LEN)).expect("create fail");

			let buf0 = gen_rand_data(18);
			let buf1 = gen_rand_data(8);
			let buf2 = gen_rand_data(4);
			let buf3 = gen_rand_data(8);

			qf.add(&buf0).expect("add buf0 fail");
			qf.add(&buf1).expect("add buf1 fail");

			assert_eq!(qf.size(), 2);
			assert_eq!(qf.remaining_bytes(), 2);
	
			qf.remove().expect("remove buf0");

			qf.add(&buf2).expect("wrapping add buf2");
			assert_eq!(qf.remaining_bytes(), 16);

			qf.add(&buf3).expect("add buf3");

			let vpeek = qf.peek().unwrap().unwrap();
			assert_eq!(&vpeek, &buf1);

			qf.remove().expect("remove buf1");

			let vpeek = qf.peek().unwrap().unwrap();
			assert_eq!(&vpeek, &buf2);

			qf.remove().expect("remove buf2");

			let vpeek = qf.peek().unwrap().unwrap();
			assert_eq!(&vpeek, &buf3);			

			qf.remove().expect("remove buf3");

			assert!(qf.is_empty());
		});
	}

	#[test]
	fn rawblockdev_overadd() {
		const FNAME: &str = "qf-v2-raw3.img";
		const FILE_LEN: u64 = 60;

		use crate::tests::gen_rand_data;
		// the V2 format as a 36 byte qf header, 8 byte (len, crc) element header
		use crate::Error;		

		run_w_fixedfile(FNAME, FILE_LEN, || {
			let mut qf = QueueFile::create(FNAME, Some(FILE_LEN)).expect("create fail");

			let buf0 = gen_rand_data(10);
			let buf1 = gen_rand_data(8);
			let buf2 = gen_rand_data(8);			

			qf.add(&buf0).expect("add buf0 fail");
			let res = qf.add(&buf1);
			assert!(matches!(res, Err(Error::NoSpace {})));

			assert_eq!(qf.size(), 1);
			assert_eq!(qf.remaining_bytes(), 6);
	
			qf.remove().expect("rem err");

			qf.add(&buf2).expect("add less");

			assert_eq!(qf.remaining_bytes(), 8);
		});
	}	

    const ITERATIONS: usize = 100;
    const MIN_N: usize = 1;
    const MAX_N: usize = 10;
    const MIN_DATA_SIZE: usize = 0;
    const MAX_DATA_SIZE: usize = 4096;
    const CLEAR_PROB: f64 = 0.05;
    const REOPEN_PROB: f64 = 0.01;

    #[test]
    fn sim_use_rawblockdev() {
        const FILE_LEN: u64 = 1024*1024;
        let (path, qf) = {
            let file_name = gen_rand_file_name();
            let path = Path::new(file_name.as_str());
            // a raw block device expects a dev of that size in existence already
            {
                let fraw = std::fs::File::create(&path).expect("create rawfile");
                dbg!(&fraw);
                fraw.set_len(FILE_LEN).expect("extend size");
                fraw.sync_all().expect("sync");
            }

            let qf = QueueFile::create(&path, Some(FILE_LEN)).expect("creating qf rawblockdev");
            (path.to_owned(), qf)
        };
        simulate_use(
            &path,
            qf,
            ITERATIONS,
            MIN_N,
            MAX_N,
            MIN_DATA_SIZE,
            MAX_DATA_SIZE,
            CLEAR_PROB,
            REOPEN_PROB,
        );
        std::fs::remove_file(&path).unwrap_or(());
    }	
}
