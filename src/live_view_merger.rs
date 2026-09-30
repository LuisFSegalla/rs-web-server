use actix_web::web::head;
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};
use clap::Parser;
use serde_json;
use serde::{Serialize, Deserialize};
use bytemuck::cast_slice;
use crate::server::Header;

mod server;
# [derive(Serialize,Deserialize)]
struct TmpHeader {
    pub acquisition_id: String,
    pub frame_num: i32,
    pub shape: (String,String,String),
}



pub struct LiveViewCombiner {
    pub pub_addr: String,
    pub sub_addr: Vec<String>,
    pub_socket: zeromq::PubSocket,
    sub_socket: Vec<zeromq::SubSocket>,
}

impl LiveViewCombiner {
    pub async fn new(pub_addr: String, sub_addrs: Vec<String>) -> Result<LiveViewCombiner, zeromq::ZmqError> {
        log::info!("Trying to stabilish connection with pub port: {pub_addr}");
        let mut pub_socket = zeromq::PubSocket::new();
        match pub_socket.bind(&format!("tcp://0.0.0.0:{pub_addr}")).await {
            Ok(_) => {
                log::info!("Connected to tcp://0.0.0.0:{pub_addr}");
            }
            Err(e) => {
                log::error!("Failed to connect to tcp://0.0.0.0:{pub_addr}\n{e}");
                return Err(e);
            }
        }
        let mut v: Vec<zeromq::SubSocket> = Vec::with_capacity(sub_addrs.len());
        for addr in &sub_addrs {
            log::info!("Trying to stabilish connection with sub address: {addr}");
            let mut sub_socket: zeromq::SubSocket = zeromq::SubSocket::new();
            match sub_socket.connect(addr).await {
                Ok(_) => {
                    log::info!("Connected to {addr}");
                }
                Err(e) => {
                    log::error!("Failed to connect to {addr}\n{e}");
                    return Err(e);
                }
            }
            match sub_socket.subscribe("").await {
                Ok(_) => log::debug!("Subscribed to all messages"),
                Err(e) => {
                    log::error!("Failed to subscribe: {e}");
                    return Err(e);
        }
            }
            v.push(sub_socket);
        }
        Ok(
            LiveViewCombiner {
                pub_addr: pub_addr,
                sub_addr: sub_addrs,
                pub_socket: pub_socket,
                sub_socket: v
            }
        )
    }

    pub async fn listen(&mut self) -> Result<(), Box<dyn std::error::Error>>{
        let mut data_vec: Vec<i32> = Vec::new();
        let mut channels: i32 = 0;
        let mut acq_id: String = "".to_string();
        let mut num: i32 = 0;
        loop {
            for socket in &mut self.sub_socket {
                match socket.recv().await {
                    Ok(msg) => {
                        let split_message = msg.into_vec();
                        match str::from_utf8(&split_message[0]) {
                            Ok(reply) => {
                                match serde_json::from_str::<TmpHeader>(&reply) {
                                    Ok(header) => {
                                        match header.shape.0.parse::<i32>() {
                                            Ok(chan) => {
                                                channels += chan;
                                            }
                                            Err(e) => {
                                                log::error!("Could not covert channels to i32: {e}")
                                            }
                                        }
                                        acq_id = header.acquisition_id;
                                        num = header.frame_num;
                                        let data: &[i32] = cast_slice(&split_message[1]);
                                        data_vec.extend(data.to_vec());
                                        log::debug!("header: id = {0} frame_num = {1}",acq_id, num);
                                    }
                                    Err(e) => {
                                        log::error!("Could not Deserialize {reply} into header: {e}")
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("Could not Deserialize reply: {e}")
                            }
                        }

                    }
                    Err(e) => {
                        log::error!("Error receiving subscription from: {:?}", e)
                    }
                }
            }
            
            let header_msg: Header = Header { 
                acquisition_id: acq_id.clone(), 
                frame_num: num, 
                shape: (channels, 4096) 
            };

            let json_header = serde_json::to_string(&header_msg)?;
            let json_bytes = serde_json::to_vec(&data_vec)?;
            log::debug!("Sending header: {:?} and frame of size {1}",header_msg, data_vec.len());
            let mut message: ZmqMessage = ZmqMessage::from(json_header);
            message.push_back(json_bytes.into());
            self.pub_socket.send(message).await?;
            data_vec.clear();
            channels = 0;
        }
    }
}


/// Frame merger application.
/// Will bundle ZMQ frames together into a full frame to be sent to web viewer
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Address of the live view server to get data from
    #[arg(
        short,
        long, 
        default_value_t="15510".to_owned()
    )]
    pub_port: String,

    /// Address to host webserver
    #[arg(
        short, 
        long,
        num_args=1.., 
        action = clap::ArgAction::Append, 
        default_values_t=["15500".to_owned(),"15501".to_owned()]
    )]
    sub_addr: Vec<String>,
}

# [tokio::main]
async fn main()-> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args: Args = Args::parse();
    let pub_port: String = args.pub_port;
    let sub_addrs: Vec<String> = args.sub_addr;

    match LiveViewCombiner::new(pub_port, sub_addrs).await {
        Ok(mut combiner) => {
            log::info!("Combiner created successfully");
            combiner.listen().await?;
        }
        Err(e) => {
            eprintln!("Failed to create LiveViewCombiner: {e}");
        }
    }
    Ok(())
}