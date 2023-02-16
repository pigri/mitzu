Mitzu webapp
============

Besides using notebooks Mitzu has it's own standalone webapp.

Starting the Mitzu webapp locally
---------------------------------

The easiest way to start the Mitzu webapp is running the following commands in the root of the Mitzu's repository:

.. code-block:: shell

   make serve

This will launch the webapp listening on the localhost:8082.

Authentication
--------------

By default the Mitzu webapp does not have any authentication and everyone can access it.

By setting the ``AUTH_BACKEND`` environmental variable the OAuth backend can be selected and configured with the backend specific environmental variables.

As of now the following authentication backends can be selected:
 - :ref:`AWS Cognito`
 - :ref:`Google OAuth`
 - :ref:`Custom OAuth backend` (requires some coding)

AWS Cognito
-----------

Mitzu can be configured to use `AWS Cognito User Pools <https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-identity.html>`_ as an authentication backend.

You may supervise the ``serve_cognito_sso`` target in the ``Makefile``.

Configure AWS Cognito for Mitzu
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 1. Open the AWS Cognito service on AWS console (`link <https://eu-west-1.console.aws.amazon.com/cognito/v2/idp/user-pools?region=eu-west-1>`_ and you may change the region)
 2. Create a new User Pool (MFA, password policy, account recovery is all up to you)
 3. For the app client set the followings:

   - Allowed callback URLs: ``<your Mitzu base url>/auth/oauth`` (eg. ``http://localhost:8082/auth/oauth``)
   - Allowed sign-out URLs: ``<your Mitzu base url>/auth/unauthorized`` (eg. ``http://localhost:8082/auth/unauthorized``)
   - OAuth grant types: Authorization code grant
   - OpenID Connect scopes: ``aws.cognito.signin.user.admin``, ``email``, ``openid``, ``profile``

Configure Mitzu to use AWS Cognito
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once AWS Cognito User Pool is configured the Mitzu webapp can be easily configured. The ``AUTH_BACKEND`` environmental variable should be set to ``cognito``,
this will configure the authorizer layer using the Cognito specific settings using the :meth:`Cognito.get_config <mitzu.webapp.auth.cognito.Cognito.get_config>` method.

.. autoclass:: mitzu.webapp.auth.cognito.Cognito
    :members:

Google OAuth
------------

Mitzu can be configured to use `Google authentication <https://cloud.google.com/docs/authentication>`_.

You may supervise the ``serve_google_sso`` target in the ``Makefile``.

Configure Google OAuth for Mitzu
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 1. Create a `new Google Project <https://console.cloud.google.com/projectcreate>`_ (or use one of your existing project)
 2. On the credentials page create a `new OAuth Client ID <https://console.cloud.google.com/apis/credentials/oauthclient>`_
   
   - Application Type: Web Application
   - Authorized redirect URIs:  ``<your Mitzu base url>/auth/oauth`` (eg. ``http://localhost:8082/auth/oauth``)

 3. Configure the OAuth consent screen, for scopes enable: `openid` and `.../auth/userinfo.email`


Configure Mitzu to use Google OAuth
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the Google project is configured the Mitzu webapp can be easily configured. The ``AUTH_BACKEND`` environmental variable should be set to ``google``,
this will configure the authorizer layer using the Cognito specific settings using the :meth:`GoogleOAuth.get_config <mitzu.webapp.auth.google.GoogleOAuth.get_config>` method.

.. autoclass:: mitzu.webapp.auth.google.GoogleOAuth
    :members:

Custom OAuth backend
--------------------

Any OAuth backend can be added easily just need to create a new :class:`OAuthConfig <mitzu.webapp.auth.authorizer.OAuthConfig>` instance.


.. autoclass:: mitzu.webapp.auth.authorizer.OAuthConfig
    :members:

