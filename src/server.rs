use zeromq::{Socket, SocketRecv};
use std::error::Error;
use std::sync::{Arc, Mutex};
use actix_web::web;
use serde::{Serialize, Deserialize};
use tokio::{sync::broadcast};
use actix::{Actor, ActorContext, AsyncContext, Message};
use actix_web_actors::ws;

# [derive(Clone)]
pub struct UserDataProtected {
    pub data_struct: Arc<Mutex<UserData>>,
}

# [derive(Serialize,Deserialize, Debug, Clone)]
pub struct UserData {
    pub name: String,
    pub id: i32,
    pub shape: (i32,i32),
    pub data: Vec<f64>

}


# [derive(Message, Clone)]
# [rtype(result = "()")]
pub struct DataUpdate {
    pub data: UserData
}


// Creating a new WebSocket struct that will broacast my whole
// UserData structure wrapped around the DataUpdate struct.
pub struct WebSocket {
    pub tx: broadcast::Sender<UserData>
}

impl Actor for WebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        log::info!("WebSocket connected!");
        
        // Subscribe to the broacast transmition
        let mut rx: broadcast::Receiver<UserData> = self.tx.subscribe();
        let addr: actix::prelude::Addr<WebSocket> = ctx.address();
        
        actix::spawn(async move {
            while let Ok(data) = rx.recv().await {
                // let json = serde_json::to_string(&data.data).unwrap();
                addr.do_send(
                    DataUpdate{
                        data: data
                    }
                );
            }
        });
    }
}

impl actix::StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebSocket {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(ws::Message::Close(_)) => ctx.stop(),
            _ => {}
        }
    }
}


impl actix::Handler<DataUpdate> for WebSocket {
    type Result = ();

    fn handle(&mut self, msg: DataUpdate, ctx: &mut Self::Context) -> Self::Result {
        match serde_json::to_string(&msg.data) {
            Ok(json) => {
                log::debug!("Handling json");
                ctx.text(json);
            }
            Err(e) => {
                eprintln!("Error in handle for WebSocket: {:?}", e);
            }
        }
    }
}


pub async fn server(url: String, tx: broadcast::Sender<UserData>) {
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
                            Ok(data) => {
                                log::info!("Decoded frame: {}", data.id);
                                let _ = tx.send(data);
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