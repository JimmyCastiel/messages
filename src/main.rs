#[macro_use] extern crate rocket;
use rocket::State;
use rocket::http::Status;
use rocket::serde::{
    Deserialize,
    Serialize,
    json::{
        to_string,
        Json
    }
};
use uuid7::uuid7;

use env;

use std::collections::LinkedList;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::message::{
    Header,
    OwnedHeaders
};
use rdkafka::producer::{
    FutureProducer,
    FutureRecord
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
struct Message {
    oa: String,
    da: String,
    body: String
}

type Messages = LinkedList<Message>;

#[post("/", data = "<message>")]
async fn send_message(message: Json<Message>, state: &State<FutureProducer>) -> (Status, Json<String>) {
    info!("{:?}", message);
    // TODO implement authentication
    let message_id: String = uuid7().to_string();
    // TODO implement checks
    let m: String = to_string(&message.into_inner()).unwrap();
    let response = state
        .send(
                    FutureRecord::to("test")
                        .payload(&m)
                        .key(&message_id)
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                )
                .await;
    match response {
        Ok(_) => (Status::Accepted, Json(message_id)),
        _ => (Status::InternalServerError, Json("".to_string()))
    }
}

#[get("/<message_id>")]
fn get_message(message_id: &str) -> Json<Message> {
    info!("message with id {} was requested", message_id);
    // TODO implement authentication
    Json(Message{oa: "".to_string(), da: "".to_string(), body: "".to_string()})
}

#[get("/")]
fn list_messages() -> Json<Messages> {
    // TODO implement authentication
    Json(LinkedList::new())
}

#[launch]
fn rocket() -> _ {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", env::var("KAFKA_BROKERS").unwrap().parse::<String>().unwrap())
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");
    rocket::build()
        .mount("/", routes![list_messages, get_message, send_message])
        .manage(producer)
}
