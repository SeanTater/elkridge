extern crate fuse;

fn main() {
    println!("Hello, world!");
    fuse::mount(SquirrelFS{}, &"/tmp/squirrelfs", &[])
        .unwrap();
}

struct SquirrelFS {}
impl fuse::Filesystem for SquirrelFS {
    
}
