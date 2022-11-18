import argparse
import os
import sys

import mitzu.model as M
from mitzu.samples.data_ingestion import create_and_ingest_sample_project

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Create an example project")
    parser.add_argument(
        "--project-dir",
        dest="project_dir",
        required=True,
        help="Path to the projects directory",
    )
    parser.add_argument(
        "--overwrite-records",
        dest="overwrite_records",
        action="store_true",
        default=True,
        help="Overwrite the stored records",
    )
    parser.add_argument(
        "--user-count",
        dest="number_of_users",
        required=False,
        default=1000,
        help="The number of users in the test data, def: 1000",
    )
    parser.add_argument(
        "--event-count",
        dest="event_count",
        required=False,
        default=100000,
        help="The total number of events the test data in all tables, def: 100000",
    )
    parser.add_argument(
        "--adapter", required=True, help="Adapater: mysql, postgresql or file"
    )

    args = parser.parse_args()

    if args.adapter == "mysql":
        connection = M.Connection(
            connection_type=M.ConnectionType.MYSQL,
            host="localhost",
            secret_resolver=M.ConstSecretResolver("test"),
            user_name="test",
            port=3307,
            schema="test",
        )
        project_name = "sample_mysql"
    elif args.adapter == "postgresql":
        connection = M.Connection(
            connection_type=M.ConnectionType.POSTGRESQL,
            host="localhost",
            secret_resolver=M.ConstSecretResolver("test"),
            user_name="test",
        )
        project_name = "sample_postgresql"
    elif args.adapter == "trino":
        connection = M.Connection(
            connection_type=M.ConnectionType.TRINO,
            user_name="test",
            secret_resolver=None,
            schema="tiny",
            catalog="minio",
            host="localhost",
        )
        project_name = "example_project_postgresql"
    elif args.adapter == "file":
        connection = M.Connection(
            connection_type=M.ConnectionType.FILE,
            extra_configs={
                "file_type": "parquet",
                "path": args.project_dir,
            },
        )
        project_name = "sample_sqlite"
    else:
        print(f"Invalid adapter type: {args.adapter}")
        sys.exit(1)

    os.makedirs(args.project_dir, exist_ok=True)

    project = create_and_ingest_sample_project(
        connection,
        number_of_users=int(args.number_of_users),
        event_count=int(args.event_count),
        overwrite_records=args.overwrite_records,
    )

    project.validate()

    dp = project.discover_project()
    dp.save_to_project_file(project_name, folder=args.project_dir)
