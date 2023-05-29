Connecting to a data warehouse
==============================

Connection Type
---------------

Before creating a :class:`Connection <mitzu.model.Connection>` the connection type should be selected. The :class:`ConnectionType <mitzu.model.ConnectionType>` enumeration contains all the supported connection types.

.. autoclass:: mitzu.model.ConnectionType
   :members:
   :undoc-members:

Connection
----------

Mitzu establishes the connection to the data warehouse or lake with `SQL Alchemy <https://www.sqlalchemy.org/>`_. The :class:`Connection <mitzu.model.Connection>` class contains the relevant information for reaching the data warehouse or lake.

.. autoclass:: mitzu.model.Connection
   :members:

.. code-block:: python

    import mitzu.model as M

    connection = M.Connection(
        connection_type = M.ConnectionType.TRINO,
        host = "host",
        port = 443,
        user_name = "user_name",
        secret_resolver = M.ConstSecretResolver("Password"),
        catalog = "catalog",
        schema = "schema",
        extra_configs = {}
    )

Secret Resolvers
----------------

.. autoclass:: mitzu.model.SecretResolver
   :members:

To avoid hardcoding secrets to code Mitzu provides the :class:`SecretResolver <mitzu.model.SecretResolver>` classes.

.. autoclass:: mitzu.model.ConstSecretResolver
   :members:

Mitzu gets and caches the secret from the :class:`SecretResolver <mitzu.model.SecretResolver>` at the first query execution.

More secret resolvers are coming soon. The aim is to integrate with most cloud provider credential storages.