from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Optional


REDIRECT_TO_COOKIE = "redirect_to"
HOME_URL = os.getenv("HOME_URL")
MITZU_WEBAPP_URL = os.getenv("MITZU_WEBAPP_URL")
SIGN_OUT_URL = os.getenv("SIGN_OUT_URL")

UNAUTHORIZED_URL = "/auth/unauthorized"

class MitzuAuthorizer(ABC):
    @abstractmethod
    def get_user_email(self, encoded_token: str) -> Optional[str]:
        pass


class GuestAuthorizer(MitzuAuthorizer):
    def get_user_email(self, _: str) -> Optional[str]:
        return "Guest"
