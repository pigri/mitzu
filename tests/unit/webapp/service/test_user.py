import pytest
from typing import Optional

from mitzu.webapp.service.user_service import (
    UserService,
    UserAlreadyExists,
    UserNotFoundException,
    UserPasswordAndConfirmationDoNotMatch,
    RootUserCannotBeChanged,
)
from mitzu.webapp.model import Role
from tests.unit.webapp.fixtures import InMemoryCache
import mitzu.webapp.configs as configs
import mitzu.webapp.storage as S


def create_service(root_password: Optional[str] = None) -> UserService:
    storage = S.MitzuStorage(InMemoryCache())
    return UserService(storage, root_password=root_password)


def test_service_creates_root_user_if_password_is_set():
    service = create_service()
    assert len(service.list_users()) == 0

    service = create_service(root_password="test")
    assert len(service.list_users()) == 1
    root_user = service.get_user_by_email_and_password(
        configs.AUTH_ROOT_USER_EMAIL, "test"
    )
    assert root_user is not None
    assert root_user.email == configs.AUTH_ROOT_USER_EMAIL


def test_create_new_user():
    service = create_service()
    assert len(service.list_users()) == 0

    email = "a@b.c"
    with pytest.raises(UserPasswordAndConfirmationDoNotMatch):
        service.new_user(email, "password", "password2")

    user_id = service.new_user(email, "password", "password")
    user = service.get_user_by_id(user_id)
    assert user is not None
    assert user.email == email

    with pytest.raises(UserAlreadyExists):
        service.new_user(email, "password", "password")


def test_user_lookup_with_password():
    service = create_service()
    email = "a@b.c"
    password = "password"

    assert service.get_user_by_email(email) is None
    assert service.get_user_by_email_and_password(email, password) is None

    service.new_user(email, password, password)

    user_by_email = service.get_user_by_email(email)
    user_by_email_and_password = service.get_user_by_email_and_password(email, password)
    assert user_by_email is not None
    assert user_by_email == user_by_email_and_password

    assert service.get_user_by_email_and_password(email, "wrong password") is None


def test_root_user_cannot_be_deleted():
    service = create_service(root_password="password")
    root_user = service.get_user_by_email(configs.AUTH_ROOT_USER_EMAIL)

    with pytest.raises(RootUserCannotBeChanged):
        service.delete_user(root_user.id)


def test_update_password():
    service = create_service()
    email = "a@b.c"
    password = "password"

    with pytest.raises(UserNotFoundException):
        service.update_password("id", password, password)

    user_id = service.new_user(email, "a", "a")
    assert service.get_user_by_id(user_id).email == email

    with pytest.raises(UserPasswordAndConfirmationDoNotMatch):
        service.update_password(user_id, password, "different-" + password)

    service.update_password(user_id, password, password)
    assert service.get_user_by_email_and_password(email, password) is not None


def test_update_role():
    service = create_service(root_password="password")
    email = "a@b.c"
    password = "password"

    with pytest.raises(UserNotFoundException):
        service.update_role("id", Role.ADMIN)

    with pytest.raises(RootUserCannotBeChanged):
        root_user = service.get_user_by_email(configs.AUTH_ROOT_USER_EMAIL)
        service.update_role(root_user.id, Role.MEMBER)

    user_id = service.new_user(email, password, password, role=Role.MEMBER)
    user = service.get_user_by_id(user_id)
    assert user is not None
    assert user.role == Role.MEMBER

    service.update_role(user_id, Role.ADMIN)
    user = service.get_user_by_id(user_id)
    assert user.role == Role.ADMIN


def test_delete_user():
    service = create_service()
    email = "a@b.c"

    with pytest.raises(UserNotFoundException):
        service.delete_user("id")

    user_id = service.new_user(email, "password", "password")
    assert service.get_user_by_id(user_id) is not None

    service.delete_user(user_id)
    assert service.get_user_by_id(user_id) is None


def test_delete_root_user():
    service = create_service(root_password="password")
    root_user = service.get_user_by_email(configs.AUTH_ROOT_USER_EMAIL)

    with pytest.raises(RootUserCannotBeChanged):
        service.delete_user(root_user.id)


def test_is_root_user():
    service = create_service(root_password="password")
    root_user = service.get_user_by_email(configs.AUTH_ROOT_USER_EMAIL)

    assert service.is_root_user(root_user.id)

    user_id = service.new_user("new_user@mail.com", "password", "password")
    assert not service.is_root_user(user_id)
