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
    match socket.connect(&url).await {
        Ok(_) => log::info!("Connected to {url}"),
        Err(e) => {
            log::error!("Failed to connect to {url}: {e}");
            return;
        }
    }
    match  socket.subscribe("").await {
        Ok(_) => log::info!("Subscribed to all messages"),
        Err(e) => {
            log::error!("Failed to subscribe: {e}");
            return;
        }
    }
    log::info!("Connection stabilished with {url}");
        
    loop {
        match socket.recv().await {
            Ok(msg) => {
                match msg.try_into() {
                    Ok(repl) => {
                        let repl: String = repl;
                        match serde_json::from_str::<UserData>(&repl) {
                            Ok(json) => {
                                match data.data_struct.lock() {
                                    Ok(mut _data) => {
                                        _data.id = json.id;
                                        _data.name = json.name;
                                        _data.data = json.data;
                                        _data.shape = json.shape;
                                        log::info!("data {:?}",_data.id);
                                    }
                                    Err(e) => log::error!("Failed to lock data: {e}"),
                                }
                            }
                            Err(e) => log::error!("Could not Deserialize reply: {e}"),
                            
                        }
                    }
                    Err(e) => log::error!("Could not convert msg: {e}"),
                }

            }
            Err(e) => {
                eprintln!("Error receiving subscription: {:?}", e);
                break;
            }
        }
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