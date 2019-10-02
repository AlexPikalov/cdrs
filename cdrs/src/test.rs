#[macro_export]
macro_rules! fixture {
    ($fxt:expr) => {
        {
            use std::fs::File;
            use std::path::Path;
            use std::io::Read;
            File::open(Path::new("./fixtures").join($fxt).as_path()).map(|mut f| {
                let mut bytes = vec![];
                f.read_to_end(&mut bytes).unwrap();
                bytes
            })
        }
    }
}
