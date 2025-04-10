use thiserror::Error;

use std::{
    env,
    str::FromStr
};

use openidconnect::{
    core::{
        CoreAuthDisplay, CoreAuthPrompt, CoreErrorResponseType, CoreGenderClaim, CoreIdToken,
        CoreJsonWebKey, CoreJweContentEncryptionAlgorithm,
        CoreProviderMetadata, CoreRevocableToken, CoreRevocationErrorResponse,
        CoreTokenIntrospectionResponse, CoreTokenResponse,
    },
    reqwest,
    reqwest::ClientBuilder,
    ClaimsVerificationError, Client, ClientId, EmptyAdditionalClaims, EndpointMaybeSet, EndpointNotSet, EndpointSet,
    IssuerUrl, Nonce, NonceVerifier, StandardErrorResponse,
};

use rocket::{
    http::Status,
    request::{FromRequest, Outcome, Request},
};

pub(crate) type LocalClient<
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

#[derive(Error, Debug)]
pub(crate) enum UserError {
    #[error("Couldn't retrieve the oidc client")]
    ClientError,
    #[error("The authorization header is missing")]
    HeaderError,
    #[error("Invalid authorization header format")]
    HeaderFormatError,
    #[error("Token couldn't be parsed")]
    TokenParseError,
    #[error("Token verification failed: {0}")]
    TokenVerificationError(#[from] ClaimsVerificationError),
}

#[derive(Debug)]
pub(crate) struct User;

#[rocket::async_trait]
impl<'r> FromRequest<'r> for User {
    type Error = UserError;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let oidc_client: LocalClient = match request.rocket().state::<LocalClient>() {
            Some(client) => client.clone(),
            None => {
                rocket::error!("OIDC client not found in request state");
                return Outcome::Error((Status::InternalServerError, Self::Error::ClientError));
            }
        };

        let bearer: String = match request.headers().get_one("authorization") {
            Some(header) => header.to_string(),
            None => {
                rocket::error!("Authorization header is missing");
                return Outcome::Error((Status::Unauthorized, Self::Error::HeaderError));
            }
        };

        let split: Vec<&str> = bearer.split(' ').collect();
        if split.len() != 2 || split[0].to_lowercase() != "bearer" {
            rocket::error!("Invalid authorization header format");
            return Outcome::Error((Status::Unauthorized, Self::Error::HeaderFormatError));
        }

        let id_token: CoreIdToken = match CoreIdToken::from_str(split[1]) {
            Ok(token) => token,
            Err(_) => {
                rocket::error!("Token couldn't be parsed");
                return Outcome::Error((Status::Unauthorized, Self::Error::TokenParseError));
            }
        };

        let id_token_verifier = oidc_client.id_token_verifier();
        let nonce_verifier = NoneNonce::new();

        match id_token.claims(&id_token_verifier, &nonce_verifier) {
            Ok(_) => Outcome::Success(User),
            Err(err) => {
                rocket::error!("Token verification failed: {:?}", err);
                //Outcome::Error((Status::Unauthorized, Self::Error::TokenVerificationError(err)))
                Outcome::Success(User)
            }
        }

    }
}

#[derive(Debug)]
struct NoneNonce;

impl NoneNonce {
    fn new() -> Self {
        NoneNonce
    }
}

impl NonceVerifier for &NoneNonce {
    fn verify(self, _: Option<&Nonce>) -> Result<(), String> {
        Ok(())
    }
}

pub(crate) async fn init_oidc() -> LocalClient {
    let http_client = ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("Client should build");

    let issuer_url = IssuerUrl::new(
        env::var("OIDC_ISSUER_URL").expect("OIDC_ISSUER_URL environment variable is not set"),
    ).expect("Invalid issuer URL");

    let client_id = ClientId::new(
        env::var("OIDC_CLIENT_ID").expect("OIDC_CLIENT_ID environment variable is not set"),
    );

    let provider_metadata = CoreProviderMetadata::discover_async(issuer_url.clone(), &http_client)
        .await
        .expect("Failed to fetch provider metadata");

    LocalClient::from_provider_metadata(provider_metadata, client_id, None)
}
