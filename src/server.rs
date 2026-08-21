use zeromq::{Socket, SocketRecv};
use serde::{Serialize, Deserialize};
use tokio::{sync::broadcast};
use actix::{Actor, ActorContext, AsyncContext, Message};
use actix_web_actors::ws;
use bytemuck::cast_slice;


# [derive(Serialize,Deserialize, Debug, Clone)]
pub struct UserData {
    pub acquisition_id: String,
    pub frame_num: i32,
    pub shape: (String,String),
    pub data: Vec<i32>

}

# [derive(Serialize,Deserialize)]
struct Header {
    pub acquisition_id: String,
    pub frame_num: i32,
    pub shape: (String,String),
}

# [derive(Message, Clone)]
# [rtype(result = "()")]
pub struct DataUpdate {
    pub zmq_struct: UserData
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
                addr.do_send(
                    DataUpdate{
                        zmq_struct: data
                    }
                );
            }
        });
    }
}

impl actix::StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebSocket {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        log::info!("Calling the handler function for the StreamHandler::WebSocket");
        match item {
            Ok(ws::Message::Close(_)) =>{
                log::info!("WebSocket connection closed by the client.");
                 ctx.stop()
            },
            Err(e) => {
                log::error!("WebSocket error: {:?}", e);
                ctx.stop();
            }
            _ => {}
        }            
    }
}


impl actix::Handler<DataUpdate> for WebSocket {
    type Result = ();

    fn handle(&mut self, msg: DataUpdate, ctx: &mut Self::Context) -> Self::Result {
        match serde_json::to_string(&msg.zmq_struct) {
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
        
    loop {
        match socket.recv().await {
            Ok(msg) => {
                let vec_message = msg.into_vec();
                match str::from_utf8(&vec_message[0]) {
                    Ok(repl) => {
                        match serde_json::from_str::<Header>(&repl) {
                            Ok(header) => {
                                let mca_data: &[i32] = cast_slice(&vec_message[1]);
                                let data: UserData = UserData {
                                    acquisition_id:header.acquisition_id,
                                    frame_num: header.frame_num,
                                    shape: header.shape, 
                                    data: mca_data.to_vec()
                                };
                                log::debug!("Decoded frame: {}", data.frame_num);
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
