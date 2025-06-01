mod kafka;
mod oidc;

use crate::{
    kafka::{KafkaFutureProducer, KafkaProducer, Producer},
    oidc::{init_oidc, LocalOidcClient, User},
};

#[macro_use]
extern crate rocket;
use rocket::{
    http::Status,
    serde::{
        json::{to_string, Json},
        Deserialize, Serialize,
    },
    State,
};
use uuid7::uuid7;

use std::collections::LinkedList;

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
struct Message {
    source: String,
    destination: String,
    body: String,
}

type Messages = LinkedList<Message>;

#[post("/", format = "application/json", data = "<message>")]
async fn send_message(
    _user: User,
    message: Json<Message>,
    kafka_client: &State<KafkaProducer<KafkaFutureProducer>>,
) -> (Status, Json<String>) {
    rocket::info!("{:?}", message);
    let message_id: String = uuid7().to_string();
    // TODO implement checks
    let m: String = to_string(&message.into_inner()).unwrap();

    let response = kafka_client.inner().send_message(&message_id, &m).await;

    match response {
        Ok(_) => (Status::Accepted, Json(message_id)),
        _ => (Status::ServiceUnavailable, Json("".to_string())),
    }
}

#[get("/<message_id>")]
async fn get_message(_user: User, message_id: &str) -> Json<Message> {
    rocket::info!("message with id {} was requested", message_id);
    // TODO function's body
    Json(Message {
        source: "".to_string(),
        destination: "".to_string(),
        body: "".to_string(),
    })
}

#[get("/")]
async fn list_messages(_user: User) -> Json<Messages> {
    // TODO function's body
    Json(LinkedList::new())
}

#[launch]
async fn rocket() -> _ {
    let oidc_client: LocalOidcClient = init_oidc().await;

    let producer: KafkaProducer<KafkaFutureProducer> =
        KafkaProducer::<KafkaFutureProducer>::new().expect("Couldn't create Kafka producer");

    rocket::build()
        .mount("/", routes![list_messages, get_message, send_message])
        .manage(oidc_client)
        .manage(producer)
}
