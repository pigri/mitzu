import random
from datetime import datetime, timedelta
from typing import Dict
from uuid import uuid4

import pandas as pd

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
    "sign_in",
]
subscription_reason = ["promo", "other", "referral", "unknown"]
urls = ["www.awestore.com", "www.mega-magasin.fr", "www.superstore.cn"]


def random_web_event(user: Dict) -> Dict:
    while True:
        id = random.randint(0, len(web_event_names) - 1)
        event_name = web_event_names[id]
        if random.randint(0, len(web_event_names)) >= id:
            break

    while True:
        event_time = datetime(2022, 10, 1) - timedelta(
            days=random.randrange(0, 84), seconds=random.randrange(0, 24 * 3600)
        )
        if random.randrange(0, 6) >= abs(event_time.weekday() - 3):
            break

    cart_size = None
    if event_name == "add_to_cart":
        cart_size = random.randrange(1, 8) * random.randrange(1, 8)
    purchase_event = event_name in ("add_to_cart", "checkout", "purchase")

    evt_props = {
        "url": random.choice(urls),
        "items_hash": (
            f"item_{random.randrange(1000, 10000)}" if purchase_event else None
        ),
        "price_shown": (
            random.randrange(1, 100) * random.randrange(1, 100)
            if event_name == "checkout"
            else None
        ),
        "cart_size": cart_size,
    }

    evt_props = {k: v for k, v in evt_props.items() if v is not None}

    return {
        "event_name": event_name,
        "event_time": event_time,
        "user_id": user["user_id"],
        **evt_props,
        **user,
    }


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"


def generate_web_events(
    web_event_count: int = 100000, user_count: int = 500
) -> pd.DataFrame:

    users = [random_user() for _ in range(0, user_count)]
    web_events = [
        random_web_event(random.choice(users)) for _ in range(0, web_event_count)
    ]

    return pd.DataFrame(web_events)
