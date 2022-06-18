import random
import sys
from datetime import datetime, timedelta
from typing import Dict, List
from uuid import uuid4

import pandas as pd
from faker import Faker

fake = Faker()

country_codes = [
    "cn",
    "us",
    "de",
    "fr",
    "hu",
    "gb",
    "br",
]

locales = [
    "cn_CN",
    "en_US",
    "de_DE",
    "fr_FR",
    "hu_HU",
    "en_GB",
    "br_BR",
]

campaigns = [
    "summer_sale_2020",
    "promo_20off_2020",
    "christmass_2020",
    "organic",
    "spring_sale_2020",
]


def random_user() -> Dict:
    return {
        "user_id": str(uuid4()),
        "country_code": random.choice(country_codes),
        "locale": random.choice(locales),
        "is_subscribed": random.choice([True, False]),
        "aquisition_campaign": random.choice(campaigns),
    }


web_event_names = [
    "page_visit",
    "add_to_cart",
    "checkout",
    "purchase",
    "search",
]

urls = ["www.awestore.com", "www.mega-magasin.fr", "www.superstore.cn"]


def random_web_event(user: Dict) -> Dict:
    event_name = random.choice(web_event_names)
    event_time = datetime(2021, 1, 1, 0, 0, 0) + timedelta(
        days=random.randrange(0, 84), seconds=random.randrange(0, 24 * 3600)
    )
    search_term = None
    cart_size = None
    if event_name == "search":
        search_term = fake.paragraph(nb_sentences=1)
    if event_name == "add_to_cart":
        cart_size = random.randrange(1, 8) * random.randrange(1, 8)
    purchase_event = event_name in ("add_to_cart", "checkout", "purchase")

    evt_props = {
        "url": random.choice(urls),
        "search_term": search_term,
        "items_hash": (
            f"item_{random.randrange(1000, 10000)}" if purchase_event else None
        ),
        "price_shown": (
            random.randrange(10, 1000) * random.randrange(10, 1000)
            if event_name == "checkout"
            else None
        ),
        "cart_size": cart_size,
    }

    evt_props = {k: v for k, v in evt_props.items() if v is not None}

    res = {
        "event_name": event_name,
        "user_properties": user,
        "event_time": event_time,
        "user_id": user["user_id"],
        "event_properties": evt_props,
    }

    return res


subscription_reason = ["promo", "other", "referral", "unknown"]
subs_cache: List[str] = []


def random_sub_event(users: List[Dict]) -> Dict:
    event_time = datetime(2021, 1, 1, 0, 0, 0) + timedelta(
        days=random.randrange(0, 84), seconds=random.randrange(0, 24 * 3600)
    )
    user = random.choice(users)
    while user["user_id"] is subs_cache:
        user = random.choice(users)

    return {
        "subscription_time": event_time,
        "subscriber_id": user["user_id"],
        "event_properties": {
            "reason": random.choice(subscription_reason),
        },
    }


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

if __name__ == "__main__":

    WEB_EVENT_COUNT = int(sys.argv[1])
    SUB_EVENT_COUNT = int(WEB_EVENT_COUNT / 100)
    USER_COUNT = SUB_EVENT_COUNT * 2

    users = [random_user() for _ in range(0, WEB_EVENT_COUNT)]
    web_events = [
        random_web_event(random.choice(users)) for _ in range(0, WEB_EVENT_COUNT)
    ]
    sub_events = [random_sub_event(users) for _ in range(0, SUB_EVENT_COUNT)]

    web_events_df = pd.DataFrame(web_events)
    sub_events_df = pd.DataFrame(sub_events)

    web_events_df.to_parquet(f"./web_events_{WEB_EVENT_COUNT}.parquet")
    sub_events_df.to_parquet(f"./sub_events_{SUB_EVENT_COUNT}.parquet")

    web_events_df["event_time"] = web_events_df["event_time"].dt.strftime(ISO_FORMAT)
    sub_events_df["subscription_time"] = sub_events_df["subscription_time"].dt.strftime(
        ISO_FORMAT
    )

    web_events_df.to_json(
        f"./web_events_{WEB_EVENT_COUNT}.json", orient="records", lines=True
    )
    sub_events_df.to_json(
        f"./sub_events_{SUB_EVENT_COUNT}.json", orient="records", lines=True
    )
