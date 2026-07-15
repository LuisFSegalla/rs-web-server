use actix_web::{App, HttpResponse, HttpServer, Responder, get, web};
use tera::{Context, Tera};
use std::sync::{Arc, Mutex};

mod server;


#[get("/data")]
async fn index(tera: web::Data<Tera>, data: web::Data<server::UserDataProtected>) -> impl Responder {
    let  read_data: std::sync::MutexGuard<'_, server::UserData> = data.data_struct.lock().unwrap();
    let mut ctx: Context = Context::new();
    ctx.insert("name", &read_data.name.clone());
    HttpResponse::Ok().body(tera.render("base.html", &ctx).unwrap())
}

#[get("/zmq_data")]
async fn zmq_data(tera: web::Data<Tera>, data: web::Data<server::UserDataProtected>) -> impl Responder {
    let _ = server::get_zmq_data(&data).await;
    let  read_data: std::sync::MutexGuard<'_, server::UserData> = data.data_struct.lock().unwrap();
    let mut ctx: Context = Context::new();
    ctx.insert("name", &*read_data.name); 
    ctx.insert("points", &*read_data.data);
    HttpResponse::Ok().body(tera.render("array.html", &ctx).unwrap())
}

#[get("/multi_data")]
async fn zmq_multi_data(tera: web::Data<Tera>, data: web::Data<server::UserDataProtected>) -> impl Responder {
    let _ = server::get_zmq_data(&data).await;
    let  read_data: std::sync::MutexGuard<'_, server::UserData> = data.data_struct.lock().unwrap();
    let mut ctx: Context = Context::new();
    ctx.insert("points", &*read_data.data);
    ctx.insert("name", &*read_data.name);
    ctx.insert("windows", &read_data.shape.0);
    ctx.insert("values_per_window", &read_data.shape.1);
    HttpResponse::Ok().body(tera.render("multi_window.html", &ctx).unwrap())
}

#[get("/plot")]
async fn plot(tera: web::Data<Tera>, data: web::Data<server::UserDataProtected>) -> impl Responder {
    let  read_data: std::sync::MutexGuard<'_, server::UserData> = data.data_struct.lock().unwrap();
    let mut ctx: Context = Context::new();
    ctx.insert("points", &*read_data.data);
    ctx.insert("name", &*read_data.name);
    ctx.insert("windows", &read_data.shape.0);
    ctx.insert("values_per_window", &read_data.shape.1);
    HttpResponse::Ok().body(tera.render("multi_window.html", &ctx).unwrap())
}

#[get("/demo")]
async fn demo(tera: web::Data<Tera>) -> impl Responder {
    let ctx: Context = Context::new();
    HttpResponse::Ok().body(tera.render("demo.html", &ctx).unwrap())
}

async fn set_val(path: web::Path<String>, data: web::Data<server::UserDataProtected>) -> impl Responder{
    let val: String = path.into_inner();
    let mut _data: std::sync::MutexGuard<'_, server::UserData> = data.data_struct.lock().unwrap();
    _data.name = val;
    ""
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    let data: server::UserDataProtected = server::UserDataProtected{
        data_struct: Arc::new(Mutex::new(
            server::UserData{
                name: "NoName".to_string(),
                id: 0,
                data: [0.0, 1.0, 2.0, 3.0].to_vec(),
                shape: (2,2),
            }
        ))
    };

    let web_data: web::Data<server::UserDataProtected> = web::Data::new(data);


    let thread_data: web::Data<server::UserDataProtected> = web_data.clone();

    let _ = tokio::spawn(async move {
        server::server("127.0.0.1:8081".to_string(), &thread_data).await;
    });

    let mut tera = Tera::new();
    tera.add_template_file("src/templates/base.html", Some("base.html")).unwrap();
    tera.add_template_file("src/templates/array.html", Some("array.html")).unwrap();
    tera.add_template_file("src/templates/multi_window.html", Some("multi_window.html")).unwrap();
    tera.add_template_file("src/templates/demo.html", Some("demo.html")).unwrap();

    HttpServer::new(move || {
        App::new()
        .app_data(web_data.clone())
        .app_data(web::Data::new(tera.clone()))
        .service(index)
        .service(zmq_data)
        .service(zmq_multi_data)
        .service(demo)
        .service(plot)
        .route("/{name}", web::get().to(set_val))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}