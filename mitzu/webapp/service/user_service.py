from typing import List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import random
import string
from mitzu.helper import create_unique_id
import hashlib
import mitzu.webapp.cache as C
import mitzu.webapp.configs as configs


class Role(Enum):
    ADMIN = "admin"
    MEMBER = "member"

    @classmethod
    def all_values(cls):
        return [
            Role.ADMIN,
            Role.MEMBER,
        ]


@dataclass
class User:
    """
    Container class for describing a user
    """

    email: str
    password_hash: str
    password_salt: str
    id: str = field(default_factory=create_unique_id)
    role: Role = Role.MEMBER


class UserNotFoundException(Exception):
    """
    Raised when the user is not found in the local user store
    """

    def __init__(self):
        super().__init__("User not found")


class UserAlreadyExists(Exception):
    """
    Raised when the user already exists in the local user store
    """

    def __init__(self):
        super().__init__("User already exists")


class UserPasswordAndConfirmationDoNotMatch(Exception):
    """
    Raised when the password and the password confirmation do not match
    when adding a new user or when chaning the password
    """

    def __init__(self):
        super().__init__("Password and password confirmation do not match")


class RootUserCannotBeChanged(Exception):
    """
    Raised before the root user changes
    """

    def __init__(self):
        super().__init__("Root user cannot be changed")


class UserService:
    """
    UserService provides the a single API to manage users in the local user storage
    """

    def __init__(self, cache: C.MitzuCache, root_password: Optional[str] = None):
        self._cache = cache

        has_admin = False
        for user in self.list_users():
            if user.role == Role.ADMIN:
                has_admin = True
                break

        if root_password and not has_admin:
            self.new_user(
                configs.AUTH_ROOT_USER_EMAIL,
                root_password,
                root_password,
                role=Role.ADMIN,
            )

    def is_root_user(self, user_id: str) -> bool:
        user = self.get_user_by_id(user_id)
        return user is not None and user.email == configs.AUTH_ROOT_USER_EMAIL

    def list_users(self) -> List[User]:
        result = []
        for key in self._cache.list_keys("users."):
            result.append(self._cache.get(f"users.{key}"))
        return result

    def get_user_by_id(self, user_id: str) -> Optional[User]:
        return self._cache.get(f"users.{user_id}")

    def get_user_by_email(self, email: str) -> Optional[User]:
        for user in self.list_users():
            if user.email == email:
                return user
        return None

    def _store_user(self, user: User):
        key = f"users.{user.id}"
        if self._cache.get(key):
            self._cache.clear(key)
        self._cache.put(key, user)

    def update_password(self, user_id: str, password: str, password_confirmation: str):
        user = self.get_user_by_id(user_id)
        if user is None:
            raise UserNotFoundException()

        if password != password_confirmation:
            raise UserPasswordAndConfirmationDoNotMatch()

        hash, salt = self._get_password_hash_with_salt(password)
        user.password_hash = hash
        user.password_salt = salt
        self._store_user(user)

    def update_role(self, user_id: str, role: Role):
        user = self.get_user_by_id(user_id)
        if user is None:
            raise UserNotFoundException()

        if user.email == configs.AUTH_ROOT_USER_EMAIL:
            raise RootUserCannotBeChanged()

        user.role = role
        self._store_user(user)

    def _get_password_hash_with_salt(self, password: str) -> Tuple[str, str]:
        salt = "".join(random.choice(string.printable) for i in range(10))
        password_to_hash = f"{password}:{salt}"
        hash = hashlib.sha256(password_to_hash.encode()).hexdigest()
        return (hash, salt)

    def new_user(
        self,
        email: str,
        password: str,
        password_confirmation: str,
        role: Role = Role.MEMBER,
    ) -> str:
        if self.get_user_by_email(email) is not None:
            raise UserAlreadyExists()

        if password != password_confirmation:
            raise UserPasswordAndConfirmationDoNotMatch()

        hash, salt = self._get_password_hash_with_salt(password)

        user = User(
            email=email,
            password_hash=hash,
            password_salt=salt,
            role=role,
        )
        self._store_user(user)
        return user.id

    def get_user_by_email_and_password(
        self, email: str, password: str
    ) -> Optional[User]:
        user = self.get_user_by_email(email)
        if user is None:
            return None

        password_to_hash = f"{password}:{user.password_salt}"
        hash = hashlib.sha256(password_to_hash.encode()).hexdigest()
        if hash == user.password_hash:
            return user
        return None

    def delete_user(self, user_id: str):
        user = self.get_user_by_id(user_id)
        if user is None:
            raise UserNotFoundException()

        if user.email == configs.AUTH_ROOT_USER_EMAIL:
            raise RootUserCannotBeChanged()

        self._cache.clear(f"users.{user.id}")
