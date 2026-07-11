use actix_web::{get, web, App, HttpServer, Responder, HttpResponse};
use tera::{Context, Tera};
use std::sync::Mutex;
mod server;


#[get("/data")]
async fn index(tera: web::Data<Tera>, data: web::Data<server::UserNameProtected>) -> impl Responder {
    let  read_data: std::sync::MutexGuard<'_, String> = data.name.lock().unwrap();
    let mut ctx: Context = Context::new();
    ctx.insert("name", &read_data.clone()); //Not great that I have to clone the value that is shared here
    HttpResponse::Ok().body(tera.render("base.html", &ctx).unwrap())
}

async fn set_val(path: web::Path<String>, data: web::Data<server::UserNameProtected>) -> String {
    let val: String = path.into_inner();
    let mut _data = data.name.lock().unwrap();
    *_data = val;
    format!("new data = {_data}")    
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut data: server::UserNameProtected = server::UserNameProtected{
        name: Mutex::new("NoName".to_string())
    };    
    let web_data: web::Data<server::UserNameProtected> = web::Data::new(data);

    let mut tera = Tera::new();
    tera.add_template_file("src/templates/base.html", Some("base.html")).unwrap();

    HttpServer::new(move || {
        App::new()
        .app_data(web_data.clone())
        .app_data(web::Data::new(tera.clone()))
        .service(index)
        .route("/{name}", web::get().to(set_val))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}