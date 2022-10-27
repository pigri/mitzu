import argparse
import sys
import time

import requests

if __name__ == "__main__":
    parser = argparse.ArgumentParser("waits for trino endpoint")
    parser.add_argument(
        "--max-timeout",
        dest="max_timeout",
        type=int,
        default=60,
        help="maximum timeout",
    )

    args = parser.parse_args()
    max_timeout = args.max_timeout

    waited = 0
    sleep = 2
    while waited < max_timeout:
        try:
            response = requests.get("http://localhost:8080/v1/info")
            if response.status_code == 200:
                resp_json = response.json()
                if resp_json["starting"]:
                    print("Trino is starting")
                else:
                    sys.exit(0)
        except Exception as e:
            print(f"Failed to connect the trino endpoint: {str(e)}")

        time.sleep(sleep)
        waited += sleep

    print("Trino endpoint is not available")
    sys.exit(1)
