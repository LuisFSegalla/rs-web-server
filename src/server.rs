use zeromq::{Socket, SocketRecv};
use std::error::Error;
use std::sync::Mutex;
use rand::{RngExt};
use actix_web::web;
use serde::{Serialize, Deserialize};

pub struct UserDataProtected {
    pub data: Mutex<UserData>,
}

# [derive(Serialize,Deserialize, Debug)]
pub struct UserData {
    pub name: String,
    pub id: i32,
    pub shape: (i32,i32),
    pub data: Vec<f64>

}

pub async fn server(url: String, data: &web::Data<UserDataProtected>) -> Result<(), Box<dyn Error>> {
    // let mut socket: zeromq::SubSocket = zeromq::SubSocket::new();
    // socket.connect(&url).await?;
    // socket.subscribe("").await?;

    println!("Connected to {url}");
    
    loop {
        // let repl: String = socket.recv().await?.try_into()?;
        let mut _data = data.data.lock().unwrap();
        // *_data = repl;
        let mut rng = rand::rng();
        let n: u32 = rng.random_range(0..100);
        _data.id = rng.random_range(0..100);
        _data.name = n.to_string();
        // *_data.data = 
    }
}


pub async fn get_zmq_data(data: &web::Data<UserDataProtected>) -> Result<(), Box<dyn Error>>{
    let mut socket: zeromq::SubSocket = zeromq::SubSocket::new();
    socket.connect(&"tcp://127.0.0.1:8081".to_string()).await?;
    socket.subscribe("").await?;
    println!("connected to 127.0.0.1:8081");
    let repl: String = socket.recv().await?.try_into()?;
    let json: UserData = serde_json::from_str(&repl)?;
    let mut _data= data.data.lock().unwrap(); // <- get counter's MutexGuard
    _data.id = json.id;
    _data.name = json.name;
    _data.data = json.data;
    _data.shape = json.shape;
    Ok(())
}