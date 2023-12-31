import pytest
from unittest.mock import MagicMock
from typing import Optional

from mitzu.webapp.service.user_service import (
    UserService,
    UserAlreadyExists,
    UserNotFoundException,
    UserPasswordAndConfirmationDoNotMatch,
)
from mitzu.webapp.model import Role
import mitzu.webapp.storage as S
import mitzu.webapp.service.notification_service as NS


def create_service(
    notification_service: Optional[NS.NotificationService] = None,
) -> UserService:
    storage = S.MitzuStorage()
    storage.init_db_schema()
    return UserService(storage, notification_service=notification_service)


def test_create_new_user():
    storage = S.MitzuStorage()
    storage.init_db_schema()
    assert len(storage.list_users()) == 0
    service = create_service()

    assert len(service.list_users()) == 0

    email = "a@b.c"
    with pytest.raises(UserPasswordAndConfirmationDoNotMatch):
        service.new_user(email, "password", "password2")

    assert len(service.list_users()) == 0

    user_id = service.new_user(email, "password", "password")
    assert len(service.list_users()) == 1
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


def test_update_password():
    service = create_service()
    email = "a@b.c"
    password = "password"

    with pytest.raises(UserNotFoundException):
        service.update_password("id", password, password)

    user_id = service.new_user(email, "aaaaaaaa", "aaaaaaaa")
    assert service.get_user_by_id(user_id).email == email

    with pytest.raises(UserPasswordAndConfirmationDoNotMatch):
        service.update_password(user_id, password, "different-" + password)

    service.update_password(user_id, password, password)
    assert service.get_user_by_email_and_password(email, password) is not None


def test_update_role():
    service = create_service()
    email = "a@b.c"
    password = "password"

    with pytest.raises(UserNotFoundException):
        service.update_role("id", Role.ADMIN)

    user_id = service.new_user(email, password, password, role=Role.MEMBER)
    user = service.get_user_by_id(user_id)
    assert user is not None
    assert user.role == Role.MEMBER

    service.update_role(user_id, Role.ADMIN)
    user = service.get_user_by_id(user_id)
    assert user.role == Role.ADMIN


def test_delete_user():
    ns = MagicMock()
    service = create_service(notification_service=ns)
    email = "a@b.c"

    with pytest.raises(UserNotFoundException):
        service.delete_user("id")

    user_id = service.new_user(email, "password", "password")
    assert service.get_user_by_id(user_id) is not None
    ns.user_created.assert_called_with(user_id, email)

    service.delete_user(user_id)
    assert service.get_user_by_id(user_id) is None
    ns.user_deleted.assert_called_with(user_id)
