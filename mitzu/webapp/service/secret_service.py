import secrets
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from dataclasses import dataclass

from mitzu.model import ConstSecretResolver, SecretResolver
import mitzu.webapp.configs as configs


@dataclass(frozen=True)
class EncryptedConstSecretResolver(SecretResolver):
    """
    Stores secret as an encrypted strings
    """

    encrypted_secret: bytes

    @staticmethod
    def __get_key(salt: bytes, iterations: int) -> bytes:
        key = configs.SECRET_ENCRYPTION_KEY
        if key is None:
            raise ValueError("Secret encryption key is not defined'")
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=iterations,
            backend=default_backend(),
        )
        return b64e(kdf.derive(key.encode()))

    def resolve_secret(self) -> str:
        decoded = b64d(self.encrypted_secret)
        salt, iter, token = decoded[:16], decoded[16:20], b64e(decoded[20:])
        iterations = int.from_bytes(iter, "big")
        key = self.__get_key(salt, iterations)
        return Fernet(key).decrypt(token).decode()

    @staticmethod
    def create(secret_value: str):
        salt = secrets.token_bytes(16)
        iterations = 100000
        encoded_value = secret_value.encode()
        f = Fernet(EncryptedConstSecretResolver.__get_key(salt, iterations))

        encrypted_secret = b"%b%b%b" % (
            salt,
            iterations.to_bytes(4, "big"),
            b64d(f.encrypt(encoded_value)),
        )
        return EncryptedConstSecretResolver(encrypted_secret=b64e(encrypted_secret))


class SecretService:
    def get_secret_resolver(self, secret_value: str) -> SecretResolver:
        if configs.SECRET_ENCRYPTION_KEY:
            return EncryptedConstSecretResolver.create(secret_value)

        return ConstSecretResolver(secret_value)
