mod oidc;
mod kafka;

use crate::{
    kafka::init_producer,
    oidc::{init_oidc, LocalClient, User}
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

use std::{collections::LinkedList, time::Duration};

use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
struct Message {
    source: String,
    destination: String,
    body: String,
}

type Messages = LinkedList<Message>;

#[post("/", data = "<message>")]
async fn send_message(
    _user: User,
    message: Json<Message>,
    state: &State<FutureProducer>,
) -> (Status, Json<String>) {
    info!("{:?}", message);
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
        _ => (Status::ServiceUnavailable, Json("".to_string())),
    }
}

#[get("/<message_id>")]
async fn get_message(_user: User, message_id: &str) -> Json<Message> {
    info!("message with id {} was requested", message_id);
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
    let oidc_client: LocalClient = init_oidc().await;

    let producer: FutureProducer = init_producer();

    rocket::build()
        .mount("/", routes![list_messages, get_message, send_message])
        .manage(oidc_client)
        .manage(producer)
}
