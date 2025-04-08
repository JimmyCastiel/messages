use thiserror::Error;

use env;

use std::str::FromStr;

use openidconnect::{
    core::{
        CoreAuthDisplay, CoreAuthPrompt, CoreErrorResponseType, CoreGenderClaim, CoreIdToken,
        CoreIdTokenVerifier, CoreJsonWebKey, CoreJweContentEncryptionAlgorithm,
        CoreProviderMetadata, CoreRevocableToken, CoreRevocationErrorResponse,
        CoreTokenIntrospectionResponse, CoreTokenResponse,
    },
    reqwest,
    reqwest::ClientBuilder,
    Client, ClientId, EmptyAdditionalClaims, EndpointMaybeSet, EndpointNotSet, EndpointSet,
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
    #[error("No specific reason")]
    Empty,
}

#[derive(Debug)]
pub(crate) struct User {}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for User {
    type Error = UserError;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let headers = request.headers();
        match headers.get_one("authorization") {
            Some(bearer) => {
                let oidc_client = request.rocket().state::<LocalClient>();
                match oidc_client {
                    Some(oidc_client) => {
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
                        match id_token {
                            Err(_) => {
                                error!("Token couldn't be parsed");
                                Outcome::Error((Status::Unauthorized, Self::Error::Empty))
                            }
                            Ok(id_token) => {
                                let id_token_verifier: CoreIdTokenVerifier =
                                    oidc_client.id_token_verifier();

                                let nonce_verifier: NoneNonce = NoneNonce::new();

                                let claims = id_token.claims(&id_token_verifier, &nonce_verifier);

                                match claims {
                                    Ok(claims) => Outcome::Success(User {}),
                                    Err(claims) => {
                                        error!("Token is invalid : {:?}", claims);
                                        Outcome::Error((Status::Unauthorized, Self::Error::Empty))
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        error!("Couldn't retrieve the oidc client");
                        Outcome::Error((Status::Unauthorized, Self::Error::Empty))
                    }
                }
            }
            None => {
                error!("The authorization header is missing");
                Outcome::Error((Status::Unauthorized, Self::Error::Empty))
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

pub(crate) async fn init_oidc() -> LocalClient {
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
    let issuer_url: IssuerUrl = IssuerUrl::new(
        env::var("OIDC_ISSUER_URL")
            .unwrap()
            .parse::<String>()
            .unwrap(),
    )
    .unwrap();

    // Use OpenID Connect Discovery to fetch the provider metadata.
    let provider_metadata = CoreProviderMetadata::discover_async(issuer_url.clone(), &http_client)
        .await
        .unwrap();

    LocalClient::from_provider_metadata(provider_metadata.clone(), client_id.clone(), None)
}
