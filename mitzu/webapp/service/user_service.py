from typing import List, Optional, Tuple
import random
import string
import hashlib
import mitzu.webapp.storage as S
import mitzu.webapp.configs as configs
import mitzu.webapp.model as WM


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


class UserPasswordRequirementsNotMet(Exception):
    """
    Raised when the password and the password confirmation do not match
    when adding a new user or when chaning the password
    """

    def __init__(self):
        super().__init__("Password must be at least 8 characters")


class UserService:
    """
    UserService provides the a single API to manage users in the local user storage
    """

    def __init__(self, storage: S.MitzuStorage, root_password: Optional[str] = None):
        self._storage = storage

        has_admin = False
        for user in self.list_users():
            if user.role == WM.Role.ADMIN:
                has_admin = True
                break

        if root_password and not has_admin:
            self.new_user(
                configs.AUTH_ROOT_USER_EMAIL,
                root_password,
                root_password,
                role=WM.Role.ADMIN,
            )

    def list_users(self) -> List[WM.User]:
        return self._storage.list_users()

    def get_user_by_id(self, user_id: str) -> Optional[WM.User]:
        return self._storage.get_user_by_id(user_id)

    def get_user_by_email(self, email: str) -> Optional[WM.User]:
        for user in self.list_users():
            if user.email == email:
                return user
        return None

    def update_password(self, user_id: str, password: str, password_confirmation: str):
        user = self.get_user_by_id(user_id)
        if user is None:
            raise UserNotFoundException()

        if password != password_confirmation:
            raise UserPasswordAndConfirmationDoNotMatch()

        if len(password) < 8:
            raise UserPasswordRequirementsNotMet()

        hash, salt = self._get_password_hash_with_salt(password)
        user.password_hash = hash
        user.password_salt = salt
        self._storage.set_user(user)

    def update_role(self, user_id: str, role: WM.Role):
        user = self.get_user_by_id(user_id)
        if user is None:
            raise UserNotFoundException()

        user.role = role
        self._storage.set_user(user)

    def _get_password_hash_with_salt(self, password: str) -> Tuple[str, str]:
        salt = "".join(random.choice(string.printable) for i in range(10))
        password_to_hash = f"{password}:{salt}"
        hash = hashlib.sha256(password_to_hash.encode()).hexdigest()
        return (hash, salt)

    def new_user(
        self,
        email: str,
        password: Optional[str] = None,
        password_confirmation: Optional[str] = None,
        role: WM.Role = WM.Role.MEMBER,
    ) -> str:
        if self.get_user_by_email(email) is not None:
            raise UserAlreadyExists()

        if password is not None:
            if password != password_confirmation:
                raise UserPasswordAndConfirmationDoNotMatch()

            if len(password) < 8:
                raise UserPasswordRequirementsNotMet()

            hash, salt = self._get_password_hash_with_salt(password)
        else:
            raise UserPasswordRequirementsNotMet()

        user = WM.User(
            email=email,
            password_hash=hash,
            password_salt=salt,
            role=role,
        )
        self._storage.set_user(user)
        return user.id

    def get_user_by_email_and_password(
        self, email: str, password: str
    ) -> Optional[WM.User]:
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

        self._storage.clear_user(user_id)
