use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, get, web};
use actix_web_actors::ws;
use tera::{Context, Tera};
use tokio::sync::{broadcast};
use clap::Parser;
use crate::server::UserData;

mod server;

/// web server application for live viewing of MCA data
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Address of the live view server to get data from
    #[arg(short, long, default_value_t="tcp://127.0.0.1:15510".to_owned())]
    live_server: String,

    /// Address to host webserver
    #[arg(short, long,default_value_t="127.0.0.1:8080".to_owned())]
    web_server: String,
}

#[get("/data")]
async fn index(tera: web::Data<Tera>, data: web::Data<broadcast::Sender<UserData>>) -> impl Responder {
    let mut ctx: Context = Context::new();

    match data.subscribe().recv().await {
        Ok(data) => {
            ctx.insert("points", &*data.data);
            ctx.insert("name", &*data.acquisition_id);
            ctx.insert("windows", &data.shape.0);
            ctx.insert("values_per_window", &data.shape.1);
            HttpResponse::Ok().body(tera.render("multi_window.html", &ctx).unwrap())
        }
        _ => {
            HttpResponse::Ok().body(tera.render("demo.html", &ctx).unwrap())
        }
    }
}

async fn ws(
    req: HttpRequest,
    stream: web::Payload,
    tx: web::Data<broadcast::Sender<server::UserData>>,
) -> Result<HttpResponse, actix_web::Error> {
    log::debug!("/ws route called!");
    log::debug!("req: {:?}", req);
    ws::start(
        server::WebSocket {
            tx: tx.as_ref().clone(),
        },
        &req,
        stream,
    )
}

# [get("/live")]
async fn live(tera: web::Data<Tera>) -> HttpResponse {
    let ctx = tera::Context::new();
    
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(tera.render("live_data.html", &ctx).unwrap())
}


#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let args: Args = Args::parse();
    let live_view_addr: String = args.live_server;
    let web_server_addr: String = args.web_server;
    let (tx, _rx) = broadcast::channel::<server::UserData>(100);
    let tx: web::Data<broadcast::Sender<UserData>> = web::Data::new(tx.clone());

    let thread_data: broadcast::Sender<UserData>= tx.as_ref().clone();

    let _ = tokio::spawn(async move 
    {
        server::server(
            live_view_addr,
            thread_data
            ).await;
    });

    let mut tera = Tera::new();
    tera.add_template_file("src/templates/multi_window.html", Some("multi_window.html")).unwrap();
    tera.add_template_file("src/templates/live_data.html", Some("live_data.html")).unwrap();

    let serv = HttpServer::new(move || {
        App::new()
        .app_data(tx.clone())
        .app_data(web::Data::new(tera.clone()))
        .route("/ws", web::get().to(ws))
        .service(index)
        .service(live)
    });
    match serv.bind(&web_server_addr) {
        Ok(server) => {
            log::info!("Connected web server to {web_server_addr}");
            server.run().await
        },
        Err(e) => {
            log::error!("Failed to connect to {web_server_addr}: {e}");
            Err(e)
        }
    }

}