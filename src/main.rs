use thiserror::Error;

#[macro_use]
use tokio;

#[macro_use]
extern crate rocket;
use rocket::{
    http::Status,
    request::{self, FromRequest, Outcome, Request},
    serde::{
        json::{to_string, Json},
        Deserialize, Serialize,
    },
    State,
};
use uuid7::uuid7;

use env;

use std::{collections::LinkedList, option, str::FromStr, time::Duration};

use openidconnect::{
    core::{
        CoreClient, CoreIdToken, CoreIdTokenVerifier, CoreJsonWebKey, CoreJsonWebKeySet,
        CoreJsonWebKeyUse, CoreProviderMetadata,
    },
    reqwest,
    reqwest::ClientBuilder,
    ClientId,
    ClientSecret,
    IssuerUrl, //JsonWebKey, JsonWebKeyUse,
};

use rdkafka::{
    config::ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};

// https://www.scottbrady.io/tools/jwt
const TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjNmYTAxZGU4MTMwMjNkYjk4MzlmNWI0MGNmNGI5ZGYxIn0.eyJpc3MiOiJodHRwczovL2lkcC5sb2NhbCIsImF1ZCI6Im15X2NsaWVudF9hcHAiLCJzdWIiOiI1YmU4NjM1OTA3M2M0MzRiYWQyZGEzOTMyMjIyZGFiZSIsImV4cCI6MTc0Mzg0MDA3OCwiaWF0IjoxNzQzODM5Nzc4fQ.FL6sCl7MS-Ddgv2GyNnHyzcO-HwFDGyiAj5YVkaSeUT2VOU42MmQocPwuyKzQH_RGkexeafr0jCNhbIWwyXZPg";
const ISSUER_URL: &str = "https://keycloakx.dev.cpaaseng.dev/auth/realms/operations";
static mut oidc_client: Option<CoreClient> = None;

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
struct Message {
    source: String,
    destination: String,
    body: String,
}

type Messages = LinkedList<Message>;

#[derive(Error, Debug)]
enum UserError {}

#[derive(Debug)]
struct User {}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for User {
    type Error = UserError;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        /* .. */
        Outcome::Success(User {})
    }
}

#[post("/", data = "<message>")]
async fn send_message(
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
        _ => (Status::InternalServerError, Json("".to_string())),
    }
}

#[get("/<message_id>")]
async fn get_message(message_id: &str) -> Json<Message> {
    info!("message with id {} was requested", message_id);
    // TODO implement authentication
    Json(Message {
        source: "".to_string(),
        destination: "".to_string(),
        body: "".to_string(),
    })
}

#[get("/")]
async fn list_messages() -> Json<Messages> {
    // TODO implement authentication
    Json(LinkedList::new())
}

async fn init_oidc() {
    let http_client = ClientBuilder::new()
        // Following redirects opens the client up to SSRF vulnerabilities.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("Client should build");
    let client_id: ClientId = ClientId::new("client_id".to_string());
    let issuer_url: IssuerUrl = IssuerUrl::new(ISSUER_URL.to_string()).unwrap();
    let id_token: CoreIdToken = CoreIdToken::from_str(TOKEN).unwrap();

    // Use OpenID Connect Discovery to fetch the provider metadata.
    let provider_metadata = CoreProviderMetadata::discover_async(issuer_url.clone(), &http_client)
        .await
        .unwrap();

    let client = CoreClient::from_provider_metadata(
        provider_metadata.clone(),
        client_id.clone(),
        Some(ClientSecret::new("client_secret".to_string())),
    );

    //let jwks_uri = provider_metadata.jwks_uri();
    //
    //let jwks = CoreJsonWebKeySet::fetch_async(&jwks_uri, &http_client)
    //    .await
    //    .unwrap();

    let verifier: CoreIdTokenVerifier = client.id_token_verifier();

    let signing_key: &CoreJsonWebKey = id_token.signing_key(&verifier).unwrap();

    //let algo = signing_key.signing_alg();

    //println!("{:?}", algo);
    //signing_key.verify_signature(signing_key.signing_alg().unwrap, b"", b"");

    // Verify the token
    //let claims = id_token.claims(&verifier, jwks);
}

#[tokio::main]
async fn main() {
    init_oidc().await
}

//#[launch]
//async fn rocket() -> _ {
//    let producer: FutureProducer = ClientConfig::new()
//        .set(
//            "bootstrap.servers",
//            env::var("KAFKA_BROKERS")
//                .unwrap()
//                .parse::<String>()
//                .unwrap(),
//        )
//        .set("message.timeout.ms", "5000")
//        .create()
//        .expect("Producer creation error");
//    rocket::build()
//        .mount("/", routes![list_messages, get_message, send_message])
//        .manage(producer)
//}
