import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import pandas as pd

START_DATE = datetime(2021, 1, 1)
LENGTH = 365

texts = [
    "jeans",
    "shirt",
    "blue",
    "red",
    "yellow",
    "black",
    "gray",
    "pink",
    "shoes",
    "jumper",
    "long",
    "short",
]

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
    "summer_sale",
    "promo_20off",
    "christmass_sale",
    "organic",
    "facebook_amer",
    "google_ads_global",
]


subscription_reason = ["promo_code", "organic"]

page_domains = [
    "awesomestore.com",
    "awesomestore.uk",
    "megamagasin.com",
    "awesomestore.fr",
]

page_titles = ["home", "search", "blogs_page", "deals_page"]


def random_user() -> Dict:
    return {
        "user_id": str(uuid4()),
        "user_country_code": random.choice(country_codes),
        "user_locale": random.choice(locales),
    }


def random_datetime(freq: float, phase: float) -> datetime:
    while True:
        days_to_add = random.randrange(0, LENGTH)
        if (
            random.random() < math.sin(freq * (days_to_add / 180) + phase) + 1
            and round(abs(days_to_add % 7 - 3.5)) < random.random() * 14
        ):
            break

    return START_DATE + timedelta(days=days_to_add, seconds=random.randrange(0, 86400))


def random_timedelta(range_secs: int, range_days: int = 0) -> timedelta:
    return timedelta(
        seconds=random.random() ** 2 * range_secs,
        days=random.random() ** 2 * range_days,
    )


def page_event(
    event_time: datetime,
    user: Dict,
    event_name: str,
    domain: str,
    title: str,
    item_id: Optional[str],
    acq_campaign: str,
) -> Dict:
    return {
        "event_name": event_name,
        "event_time": event_time,
        "title": title,
        "domain": domain,
        "acquisition_campaign": acq_campaign,
        "item_id": item_id,
        **user,
    }


def search_event(
    event_time: datetime,
    user: Dict,
    search_text: str,
) -> Dict:
    return {
        "event_name": "search",
        "event_time": event_time,
        "search_text": search_text,
        **user,
    }


def add_to_cart(
    event_time: datetime,
    user: Dict,
    item_id: str,
) -> Dict:
    return {
        "event_name": "add_to_cart",
        "event_time": event_time,
        "item": item_id,
        **user,
    }


def checkout(event_time: datetime, user: Dict, cost: int, num_items: int) -> Dict:
    return {
        "event_name": "checkout",
        "event_time": event_time,
        "cost_usd": cost,
        "number_of_items": num_items,
        **user,
    }


def subscribe(event_time: datetime, user: Dict, email_campaign: str) -> Dict:
    return {
        "event_name": "subscribe",
        "event_time": event_time,
        "email_campaign": email_campaign,
        **user,
    }


def email_sent(event_time: datetime, user: Dict, deal_id: str) -> Dict:
    return {
        "event_name": "email_sent",
        "event_time": event_time,
        "deal_id": deal_id,
        **user,
    }


def email_opened(event_time: datetime, user: Dict) -> Dict:
    return {
        "event_name": "email_opened",
        "event_time": event_time,
        **user,
    }


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"


@dataclass(frozen=True)
class FunnelDistribution:
    funnels_count: int
    frequency: float
    phase: float
    users: List[Dict]
    page_domain: str
    page_title: str

    acquisition_campaign: str
    churn_probability: float
    add_to_cart_probability: float
    email_open_probability: float
    email_campaign: str


def create_user_funnel(
    dist: FunnelDistribution,
    user: Dict,
    page_events: List[Dict],
    search_events: List[Dict],
    add_to_carts: List[Dict],
    checkouts: List[Dict],
    email_subscriptions: List[Dict],
    email_sent_events: List[Dict],
    email_opened_events: List[Dict],
):
    # page_visit
    event_time = random_datetime(dist.frequency, dist.phase)
    page_events.append(
        page_event(
            event_time=event_time,
            event_name="page_visit",
            user=user,
            domain=dist.page_domain,
            title=dist.page_title,
            item_id=None,
            acq_campaign=dist.acquisition_campaign,
        )
    )
    if random.random() < dist.churn_probability:
        page_events.append(
            page_event(
                event_time=event_time,
                event_name="element_clicked_on_page",
                user=user,
                domain=dist.page_domain,
                title=dist.page_title,
                item_id=str(random.randrange(1, 100)),
                acq_campaign=dist.acquisition_campaign,
            )
        )

    if random.random() > dist.churn_probability:
        return

    # search + add_to_cart
    for _ in range(0, int(random.random() ** 2 * 10)):
        event_time = event_time + random_timedelta(180)
        search_events.append(
            search_event(
                event_time=event_time,
                user=user,
                search_text=" ".join(
                    [random.choice(texts) for i in range(1, random.randint(1, 4))]
                ),
            )
        )

        if random.random() < dist.add_to_cart_probability:
            add_to_carts.append(
                add_to_cart(
                    event_time=event_time,
                    user=user,
                    item_id=f"item-{random.randrange(0, 10000)}",
                )
            )

    if random.random() < dist.churn_probability:
        return

    # checkout
    event_time = event_time + random_timedelta(300)
    checkouts.append(
        checkout(
            event_time=event_time,
            user=user,
            cost=int(random.random() ** 2 * 2000),
            num_items=int(random.random() ** 2 * 20),
        )
    )

    if random.random() < dist.churn_probability:
        return

    event_time = event_time + random_timedelta(300)
    checkouts.append(
        checkout(
            event_time=event_time,
            user=user,
            cost=int(random.random() ** 2 * 2000),
            num_items=int(random.random() ** 2 * 20),
        )
    )

    if random.random() < dist.churn_probability:
        return

    event_time = event_time + random_timedelta(300)
    email_subscriptions.append(
        subscribe(event_time=event_time, user=user, email_campaign=dist.email_campaign)
    )

    # email_sent + email_opened
    for _ in range(0, int(random.random() ** 2 * 10)):
        event_time = event_time + random_timedelta(range_days=14, range_secs=86400)
        email_sent_events.append(
            email_sent(
                event_time=event_time,
                user=user,
                deal_id=f"{dist.email_campaign}-{random.randrange(0,100)}",
            )
        )
        if random.random() < dist.email_open_probability:
            event_time = event_time + random_timedelta(range_days=3, range_secs=0)
            email_opened_events.append(email_opened(event_time=event_time, user=user))
            if random.random() < dist.churn_probability:
                break


def generate_checkout_funnels(
    dist: FunnelDistribution,
) -> Tuple[Dict[str, pd.DataFrame], int]:
    page_events: List[Dict] = []
    search_events: List[Dict] = []
    add_to_carts: List[Dict] = []
    checkouts: List[Dict] = []
    email_subscriptions: List[Dict] = []
    email_sent_events: List[Dict] = []
    email_opened_events: List[Dict] = []

    for _ in range(0, dist.funnels_count):
        user = random.choice(dist.users)
        create_user_funnel(
            dist=dist,
            user=user,
            page_events=page_events,
            search_events=search_events,
            add_to_carts=add_to_carts,
            checkouts=checkouts,
            email_opened_events=email_opened_events,
            email_sent_events=email_sent_events,
            email_subscriptions=email_subscriptions,
        )

    created_count = (
        len(page_titles)
        + len(page_events)
        + len(search_events)
        + len(add_to_carts)
        + len(checkouts)
        + len(email_subscriptions)
        + len(email_sent_events)
        + len(email_opened_events)
    )

    return (
        {
            "page_events": pd.DataFrame(page_events),
            "search_events": pd.DataFrame(search_events),
            "add_to_carts": pd.DataFrame(add_to_carts),
            "checkouts": pd.DataFrame(checkouts),
            "email_subscriptions": pd.DataFrame(email_subscriptions),
            "email_sent_events": pd.DataFrame(email_sent_events),
            "email_opened_events": pd.DataFrame(email_opened_events),
        },
        created_count,
    )


def create_all_funnels(
    user_count: int = 1000, event_count: int = 100000, seed: Optional[int] = 100
) -> Dict[str, pd.DataFrame]:
    if seed is not None:
        random.seed(seed)
    results_df: Dict[str, pd.DataFrame] = {}
    users = [random_user() for _ in range(0, user_count)]

    while event_count > 0:
        dist = FunnelDistribution(
            frequency=random.random() * 3,
            phase=random.random() * 3.6,
            users=users,
            page_domain=random.choice(page_domains),
            page_title=random.choice(page_titles),
            acquisition_campaign=random.choice(campaigns),
            churn_probability=random.random(),
            add_to_cart_probability=random.random(),
            email_open_probability=0.3,
            email_campaign=random.choice(campaigns),
            funnels_count=random.randrange(0, 10000),
        )
        dfs, created_event_cnt = generate_checkout_funnels(dist)
        event_count -= created_event_cnt
        for key, df in dfs.items():
            if key in results_df:
                df = pd.concat([results_df[key], df])
            results_df[key] = df

    return results_df
