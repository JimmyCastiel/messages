use thiserror::Error;

#[macro_use]
extern crate rocket;
use rocket::{
    http::Status,
    request::{FromRequest, Outcome, Request},
    serde::{
        json::{to_string, Json},
        Deserialize, Serialize,
    },
    State,
};
use uuid7::uuid7;

use env;

use std::{collections::LinkedList, str::FromStr, time::Duration};

use openidconnect::{
    core::{
        CoreAuthDisplay, CoreAuthPrompt, CoreClient, CoreErrorResponseType, CoreGenderClaim,
        CoreIdToken, CoreIdTokenVerifier, CoreJsonWebKey, CoreJweContentEncryptionAlgorithm,
        CoreProviderMetadata, CoreRevocableToken, CoreRevocationErrorResponse,
        CoreTokenIntrospectionResponse, CoreTokenResponse,
    },
    reqwest,
    reqwest::ClientBuilder,
    Client, ClientId, ClientSecret, EmptyAdditionalClaims, EndpointMaybeSet, EndpointNotSet,
    EndpointSet, IdTokenVerifier, IssuerUrl, Nonce, NonceVerifier, StandardErrorResponse,
};

use rdkafka::{
    config::ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};

// https://www.scottbrady.io/tools/jwt
const ISSUER_URL: &str = "https://keycloakx.dev.cpaaseng.dev/auth/realms/operations";

type LocalClient<
    HasAuthUrl = EndpointSet,
    HasDeviceAuthUrl = EndpointNotSet,
    HasIntrospectionUrl = EndpointNotSet,
    HasRevocationUrl = EndpointNotSet,
    HasTokenUrl = EndpointMaybeSet,
    HasUserInfoUrl = EndpointMaybeSet,
> = Client<
    EmptyAdditionalClaims,
    CoreAuthDisplay,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJsonWebKey,
    CoreAuthPrompt,
    StandardErrorResponse<CoreErrorResponseType>,
    CoreTokenResponse,
    CoreTokenIntrospectionResponse,
    CoreRevocableToken,
    CoreRevocationErrorResponse,
    HasAuthUrl,
    HasDeviceAuthUrl,
    HasIntrospectionUrl,
    HasRevocationUrl,
    HasTokenUrl,
    HasUserInfoUrl,
>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
struct Message {
    source: String,
    destination: String,
    body: String,
}

type Messages = LinkedList<Message>;

#[derive(Error, Debug)]
enum UserError {
    #[error("No specific reason")]
    Empty,
}

#[derive(Debug)]
struct User {}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for User {
    type Error = UserError;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        /* .. */
        let oidc_client = request.rocket().state::<LocalClient>();
        let headers = request.headers();
        if !headers.contains("Authorization") {
            error!("The authorization header is missing");
            Outcome::Error((Status::Unauthorized, Self::Error::Empty))
        } else {
            match headers.get_one("authorization") {
                Some(bearer) => {
                    let split: Vec<&str> = bearer.split(' ').collect();
                    if split.len() != 2 {
                        error!("Header is incorrect");
                        return Outcome::Error((Status::Unauthorized, Self::Error::Empty));
                    }

                    if split[0].to_lowercase() != "bearer" {
                        error!("Authorization isn't bearer");
                        return Outcome::Error((Status::Unauthorized, Self::Error::Empty));
                    }

                    let id_token: Result<CoreIdToken, _> = CoreIdToken::from_str(split[1]);
                    if !id_token.is_ok() {
                        error!("Token couldn't be parsed");
                        return Outcome::Error((Status::Unauthorized, Self::Error::Empty));
                    }
                    let id_token: CoreIdToken = id_token.unwrap();

                    if oidc_client.is_none() {
                        error!("Couldn't retrieve the oidc client");
                        return Outcome::Error((Status::Unauthorized, Self::Error::Empty));
                    }

                    let id_token_verifier: CoreIdTokenVerifier =
                        oidc_client.unwrap().id_token_verifier();

                    let nonce_verifier: NoneNonce = NoneNonce::new();

                    let claims = id_token.claims(&id_token_verifier, &nonce_verifier);

                    if claims.is_err() {
                        error!("Token is invalid : {:?}", claims);
                        return Outcome::Error((Status::Unauthorized, Self::Error::Empty));
                    }

                    Outcome::Success(User {})
                }
                _ => {
                    error!("Something wrong happened");

                    Outcome::Error((Status::Unauthorized, Self::Error::Empty))
                }
            }
        }
    }
}

#[derive(Debug)]
struct NoneNonce {}

impl NoneNonce {
    fn new() -> Self {
        NoneNonce {}
    }
}

impl NonceVerifier for &NoneNonce {
    fn verify(self, _: Option<&Nonce>) -> Result<(), String> {
        Ok(())
    }
}

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
        _ => (Status::InternalServerError, Json("".to_string())),
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
    let client_id: ClientId = ClientId::new("test".to_string());
    let issuer_url: IssuerUrl = IssuerUrl::new(ISSUER_URL.to_string()).unwrap();

    // Use OpenID Connect Discovery to fetch the provider metadata.
    let provider_metadata = CoreProviderMetadata::discover_async(issuer_url.clone(), &http_client)
        .await
        .unwrap();

    let oidc_client: LocalClient =
        CoreClient::from_provider_metadata(provider_metadata.clone(), client_id.clone(), None);

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
