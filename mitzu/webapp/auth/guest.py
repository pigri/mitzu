from typing import Optional
from mitzu.webapp.auth.authorizer import MitzuAuthorizer


class GuestAuthorizer(MitzuAuthorizer):
    def get_user_email(self, _: str) -> Optional[str]:
        return "Guest"
