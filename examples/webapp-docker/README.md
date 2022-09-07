## Setting up a Mitzu Project

### Step 1: Creating a Connection object

```python
from datetime import datetime
import mitzu.model as M

connection = M.Connection(
    connection_type=M.ConnectionType.TRINO,
    user_name="<user_name>",
    secret_resolver=M.PromptSecretResolver(title="Token"),
    # In the Webapp only M.ConstSecretResolver, M.EnvVarSecretResolver are usable
    schema="<schema_name>",
    catalog="<catalog_name>",
    host="<host>",
    port=-1, # TODO Change if not default
    extra_configs= {} # Some extre configs if needed
)
```

### Step 2: Defining Event Data Tables

```python
table_1 = M.EventDataTable.create(
            table_name="sub_events",
            event_name_alias="user_subscribe",
            event_time_field="subscription_time",
            user_id_field="subscriber_id")

table_2 = M.EventDataTable.create(
            table_name="web_events",
            event_name_field="event_name",
            event_time_field="subscription_time",
            user_id_field="subscriber_id")
```

### Step 3: Creating a Project Definition

```python
project = M.Project(
        connection=connection,
        event_data_tables=[table_1, table_2],
        default_discovery_lookback_days=10
    )
```

Test your connnection with, the following command.
This select should be executed on your cluster

```python
project.adapter.execute_query("SELECT 1;")
```

### Step 4:

In order to have event and property suggestions we need to discover the project

```python
discovered_project = project.discover_project()
```

### Step 5:

For testing the discovered project inside the notebook we need to create a notebook model

```python
m = discovered_project.create_notebook_class_model()
```

For testing run these commands one by one:

```python
m.page_visited

m.page_visited.config(time_group="total", start_dt="2022-01-01", end_dt="2022-09-01")

(m.page_visited >> m.user_subscribed).config(conv_window="3 days")
```

_Congratulations You Have Managed To Create Your First Mitzu Project_

### Step 6: Persisting the Project

By persisting we can reuse the project file in the WebApp.

```python
res = discovered_project.save_to_project_file("trino_dwh")
```

### Step 7: Setting up Mitzu-Webapp (Docker Required)

1. On your local machine create a folder: `mitzu`.
2. Inside the `mitzu` folder create another folder `projects`.
3. Download the `trino_dwh.mitzu` file to the projects folder.
4. On your local machine in a terminal enter the `mitzu` folder.
5. Alternatively you can create a `docker-compose.yml` file next to the `mitzu` folder

```yml
services:
  mitzu-webapp:
    image: imeszaros/mitzu-webapp:latest
    restart: always
    entrypoint: gunicorn -b 0.0.0.0:80 app:server
    environment:
      BASEPATH: /var/task/basepath/
      HOME_URL: http://localhost:8082/
      NOT_FOUND_URL: http://localhost:8082/not_found
    ports:
      - 8080:80
    volumes:
      - ./mitzu/:/var/task/basepath/
```

6. Run `docker-compose up`
7. Open http://localhost/ in your browser
