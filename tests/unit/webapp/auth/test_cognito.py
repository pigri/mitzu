from mitzu.webapp.auth.cognito import Cognito


def test_cognito_config():
    config = Cognito.get_config(
        pool_id="pool_id",
        region="eu-west-1",
        domain="unit.ts",
        client_id="client_id",
        client_secret="client_secret",
        redirect_url="http://unit-test/",
    )

    assert (
        config.jwks_url
        == "https://cognito-idp.eu-west-1.amazonaws.com/pool_id/.well-known/jwks.json"
    )
    assert (
        config.sign_in_url
        == "https://unit.ts/oauth2/authorize?client_id=client_id&response_type=code&scope=email+openid&redirect_uri=http://unit-test/"
    )
    assert config.token_url == "https://unit.ts/oauth2/token"
