use thiserror::Error;

use env;

use std::str::FromStr;

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
    #[error("Token is expired")]
    TokenExpired(ClaimsVerificationError),
    #[error("Token audience is invalid")]
    TokenInvalidAudience(ClaimsVerificationError),
    #[error("Token auth context is invalid")]
    TokenInvalidAuthContext(ClaimsVerificationError),
    #[error("Token auth time is invalid")]
    TokenInvalidAuthTime(ClaimsVerificationError),
    #[error("Token issuer is invalid")]
    TokenInvalidIssuer(ClaimsVerificationError),
    #[error("Token nonce is invalid")]
    TokenInvalidNonce(ClaimsVerificationError),
    #[error("Token subject is invalid")]
    TokenInvalidSubject(ClaimsVerificationError),
    #[error("Undefined error")]
    TokenOther(ClaimsVerificationError),
    #[error("Token signature is invalid")]
    TokenSignatureVerification(ClaimsVerificationError),
    #[error("Token unsupported key type")]
    TokenUnsupported(ClaimsVerificationError),
    #[error("Undefined error")]
    Undefined,
}

impl From<ClaimsVerificationError> for UserError {
    fn from(err: ClaimsVerificationError) -> Self {
        match err {
            ClaimsVerificationError::Expired(_) => UserError::TokenExpired(err),
            ClaimsVerificationError::InvalidAudience(_) => UserError::TokenInvalidAudience(err),
            ClaimsVerificationError::InvalidAuthContext(_) => UserError::TokenInvalidAuthContext(err),
            ClaimsVerificationError::InvalidAuthTime(_) => UserError::TokenInvalidAuthTime(err),
            ClaimsVerificationError::InvalidIssuer(_) => UserError::TokenInvalidIssuer(err),
            ClaimsVerificationError::InvalidNonce(_) => UserError::TokenInvalidNonce(err),
            ClaimsVerificationError::InvalidSubject(_) => UserError::TokenInvalidSubject(err),
            ClaimsVerificationError::SignatureVerification(_) => {
                UserError::TokenSignatureVerification(err)
            }
            ClaimsVerificationError::Unsupported(_) => {
                UserError::TokenUnsupported(err)
            },
            ClaimsVerificationError::Other(_) => UserError::TokenOther(err),
            _ => UserError::Undefined,
        }
    }
}

#[derive(Debug)]
pub(crate) struct User {}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for User {
    type Error = UserError;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let oidc_client = match request.rocket().state::<LocalClient>() {
            Some(client) => client,
            None => {
                rocket::error!("Couldn't retrieve the oidc client");
                return Outcome::Error((Status::Unauthorized, Self::Error::ClientError));
            }
        };

        let headers = request.headers();
        let bearer = match headers.get_one("authorization") {
            Some(bearer) => bearer,
            None => {
                rocket::error!("The authorization header is missin or isn't in the correct format");
                return Outcome::Error((Status::Unauthorized, Self::Error::HeaderError));
            }
        };

        let split: Vec<&str> = bearer.split(' ').collect();
        if split.len() != 2 || split[0].to_lowercase() != "bearer" {
            rocket::error!("Invalid authorization header format");
            return Outcome::Error((Status::Unauthorized, Self::Error::HeaderFormatError));
        }

        let id_token = match CoreIdToken::from_str(split[1]) {
            Ok(token) => token,
            Err(_) => {
                rocket::error!("Token couldn't be parsed");
                return Outcome::Error((Status::Unauthorized, Self::Error::TokenParseError));
            }
        };

        let id_token_verifier = oidc_client.id_token_verifier();
        let nonce_verifier = NoneNonce::new();

        match id_token.claims(&id_token_verifier, &nonce_verifier) {
            Ok(_) => Outcome::Success(User {}),
            Err(claims) => {
                rocket::error!("Token is invalid: {:?}", claims);
                Outcome::Error((Status::Unauthorized, claims.into()))
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

    let issuer_url: IssuerUrl = IssuerUrl::new(
        env::var("OIDC_ISSUER_URL")
            .expect("OIDC_ISSUER_URL environment variable is not set")
            .parse::<String>()
            .unwrap(),
    )
    .unwrap();

    let client_id: ClientId = ClientId::new(
        env::var("OIDC_CLIENT_ID")
            .expect("OIDC_CLIENT_ID environment variable is not set")
            .parse::<String>()
            .unwrap(),
    );

    // Use OpenID Connect Discovery to fetch the provider metadata.
    let provider_metadata = CoreProviderMetadata::discover_async(issuer_url.clone(), &http_client)
        .await
        .unwrap();

    LocalClient::from_provider_metadata(provider_metadata.clone(), client_id.clone(), None)
}
