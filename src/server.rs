use zeromq::{Socket, SocketRecv};
use std::error::Error;
use std::sync::Mutex;
use rand::{RngExt};

pub struct UserNameProtected {
    pub name: Mutex<String>,
}

pub async fn server(url: String, data: UserNameProtected) -> Result<(), Box<dyn Error>> {
    // let mut socket: zeromq::SubSocket = zeromq::SubSocket::new();
    // socket.connect(&url).await?;
    // socket.subscribe("").await?;

    println!("Connected to {url}");
    
    loop {
        // let repl: String = socket.recv().await?.try_into()?;
        let mut _data = data.name.lock().unwrap();
        // *_data = repl;
        let mut rng = rand::rng();
        let n: u32 = rng.random_range(0..100);
        *_data = n.to_string();
        println!("*_data = {_data}");
    }
}