use zeromq::{Socket, SocketRecv};
use std::error::Error;
use std::sync::{Arc, Mutex};
use actix_web::web;
use serde::{Serialize, Deserialize};

// use actix_ws::Message;
// use futures_util::{
//     StreamExt as _,
//     future::{self, Either},
// };
// use tokio::{pin, select, sync::broadcast, time::interval};


# [derive(Clone)]
pub struct UserDataProtected {
    pub data_struct: Arc<Mutex<UserData>>,
}

# [derive(Serialize,Deserialize, Debug)]
pub struct UserData {
    pub name: String,
    pub id: i32,
    pub shape: (i32,i32),
    pub data: Vec<f64>

}

pub async fn server(url: String, data: &web::Data<UserDataProtected>) {
    log::info!("Trying to connecto to {url}");
    let mut socket: zeromq::SubSocket = zeromq::SubSocket::new();
    socket.connect(&url).await.expect("Failed to connect");
    socket.subscribe("").await.expect("Could not subscribe.");
    log::info!("Connection stabilished with {url}");
        
    loop {
        match socket.recv().await {
            Ok(msg) => {
                let repl: String = msg.try_into().expect("Could not convert to string");
                println!("Received ZMQ: {:?}", repl);
                let json: UserData = serde_json::from_str(&repl).expect("err");
                let mut _data = data.data_struct.lock().unwrap();
                _data.id = json.id;
                _data.name = json.name;
                _data.data = json.data;
                _data.shape = json.shape;
                println!("data {:?}",_data);

            }
            Err(e) => {
                eprintln!("Error receiving subscription: {:?}", e);
                break;
            }
        }
        // let repl: String = socket.recv().await?.try_into()?;
        // let json: UserData = serde_json::from_str(&repl)?;
        // let mut _data = data.data_struct.lock().unwrap();
        // _data.id = json.id;
        // _data.name = json.name;
        // _data.data = json.data;
        // _data.shape = json.shape;
        // println!("data {:?}",_data);
    }
        // });
}


pub async fn get_zmq_data(data: &web::Data<UserDataProtected>) -> Result<(), Box<dyn Error>>{
    let mut socket: zeromq::SubSocket = zeromq::SubSocket::new();
    socket.connect(&"tcp://127.0.0.1:8081".to_string()).await?;
    socket.subscribe("").await?;
    println!("connected to 127.0.0.1:8081");
    let repl: String = socket.recv().await?.try_into()?;
    let json: UserData = serde_json::from_str(&repl)?;
    let mut _data= data.data_struct.lock().unwrap(); // <- get counter's MutexGuard
    _data.id = json.id;
    _data.name = json.name;
    _data.data = json.data;
    _data.shape = json.shape;
    Ok(())
}