import pytest

from mitzu.webapp.service.user_service import (
    UserService,
    UserAlreadyExists,
    UserNotFoundException,
    UserPasswordAndConfirmationDoNotMatch,
    RootUserCannotBeChanged,
)
from tests.unit.webapp.fixtures import InMemoryCache
import mitzu.webapp.configs as configs


def test_service_creates_root_user_if_password_is_set():
    service = UserService(InMemoryCache())
    assert len(service.list_users()) == 0

    service = UserService(InMemoryCache(), root_password="test")
    assert len(service.list_users()) == 1
    root_user = service.get_user_by_email_and_password(
        configs.AUTH_ROOT_USER_EMAIL, "test"
    )
    assert root_user is not None
    assert root_user.email == configs.AUTH_ROOT_USER_EMAIL


def test_create_new_user():
    service = UserService(InMemoryCache())
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
    service = UserService(InMemoryCache())
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


def test_update_user():
    service = UserService(InMemoryCache())
    email = "a@b.c"
    new_email = "a2@b.c"

    with pytest.raises(UserNotFoundException):
        service.update_user_email("id", email)

    user_id = service.new_user(email, "password", "password")
    assert service.get_user_by_id(user_id).email == email

    service.update_user_email(user_id, new_email)
    assert service.get_user_by_id(user_id).email == new_email


def test_root_user_cannot_be_deleted():
    service = UserService(InMemoryCache(), root_password="password")
    root_user = service.get_user_by_email(configs.AUTH_ROOT_USER_EMAIL)

    with pytest.raises(RootUserCannotBeChanged):
        service.delete_user(root_user.id)


def test_update_password():
    service = UserService(InMemoryCache())
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


def test_delete_user():
    service = UserService(InMemoryCache())
    email = "a@b.c"

    with pytest.raises(UserNotFoundException):
        service.delete_user("id")

    user_id = service.new_user(email, "password", "password")
    assert service.get_user_by_id(user_id) is not None

    service.delete_user(user_id)
    assert service.get_user_by_id(user_id) is None


def test_delete_root_user():
    service = UserService(InMemoryCache(), root_password="password")
    root_user = service.get_user_by_email(configs.AUTH_ROOT_USER_EMAIL)

    with pytest.raises(RootUserCannotBeChanged):
        service.delete_user(root_user.id)
