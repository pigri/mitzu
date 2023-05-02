from abc import ABC


class NotificationService(ABC):
    """
    Notification Service can be used to add custom callbacks to some events.

    Eg. during user management send out custom emails, user accounts in AWS Cognito
    """

    def user_created(self, user_id: str, email: str):
        raise NotImplementedError()

    def user_deleted(self, user_id: str):
        raise NotImplementedError()


class DummyNotificationService(NotificationService):
    """
    Dummy implementation of the Notification Service ignoring all events
    """

    def user_created(self, user_id: str, email: str):
        pass

    def user_deleted(self, user_id: str):
        pass
