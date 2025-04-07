mod oidc;

use crate::oidc::{LocalClient, User, ISSUER_URL};

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

use env;

use std::{collections::LinkedList, time::Duration};

use openidconnect::{
    core::CoreProviderMetadata, reqwest, reqwest::ClientBuilder, ClientId, IssuerUrl,
};

use rdkafka::{
    config::ClientConfig,
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
    user: User,
    message: Json<Message>,
    state: &State<FutureProducer>,
) -> (Status, Json<String>) {
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
        _ => (Status::ServiceUnavailable, Json("".to_string())),
    }
}

#[get("/<message_id>")]
async fn get_message(user: User, message_id: &str) -> Json<Message> {
    info!("message with id {} was requested", message_id);
    // TODO implement authentication
    Json(Message {
        source: "".to_string(),
        destination: "".to_string(),
        body: "".to_string(),
    })
}

#[get("/")]
async fn list_messages(user: User) -> Json<Messages> {
    // TODO implement authentication
    Json(LinkedList::new())
}

#[launch]
async fn rocket() -> _ {
    let http_client = ClientBuilder::new()
        // Following redirects opens the client up to SSRF vulnerabilities.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("Client should build");
    let client_id: ClientId = ClientId::new(
        env::var("OIDC_CLIENT_ID")
            .unwrap()
            .parse::<String>()
            .unwrap(),
    );
    let issuer_url: IssuerUrl = IssuerUrl::new(ISSUER_URL.to_string()).unwrap();

    // Use OpenID Connect Discovery to fetch the provider metadata.
    let provider_metadata = CoreProviderMetadata::discover_async(issuer_url.clone(), &http_client)
        .await
        .unwrap();

    let oidc_client: LocalClient =
        LocalClient::from_provider_metadata(provider_metadata.clone(), client_id.clone(), None);

    let producer: FutureProducer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            env::var("KAFKA_BROKERS")
                .unwrap()
                .parse::<String>()
                .unwrap(),
        )
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    rocket::build()
        .mount("/", routes![list_messages, get_message, send_message])
        .manage(oidc_client)
        .manage(producer)
}
