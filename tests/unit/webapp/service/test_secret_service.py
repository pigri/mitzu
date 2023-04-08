import pytest

from mitzu.model import ConstSecretResolver
from mitzu.webapp.service.secret_service import (
    EncryptedConstSecretResolver,
    SecretService,
)
import mitzu.webapp.configs as configs


@pytest.fixture(autouse=True)
def run_around_tests():
    yield
    configs.SECRET_ENCRYPTION_KEY = None


def test_secret_service_return_const_secret_resolver():
    configs.SECRET_ENCRYPTION_KEY = None
    service = SecretService()
    password = "password"
    resolver = service.get_secret_resolver(password)

    assert isinstance(resolver, ConstSecretResolver)
    assert resolver.resolve_secret() == password


def test_secret_service_return_encrypted_const_secret_resolver():
    configs.SECRET_ENCRYPTION_KEY = "key"
    service = SecretService()
    password = "password"
    resolver = service.get_secret_resolver(password)

    assert isinstance(resolver, EncryptedConstSecretResolver)
    assert resolver.resolve_secret() == password
