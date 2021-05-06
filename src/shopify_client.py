import json
import logging
import os
import uuid
from enum import Enum
from typing import List, Optional

import pandas as pd
import requests
from requests.exceptions import HTTPError

from .database import execute_pgsql, get_pgsql_pandas_data

logger = logging.getLogger(__name__)

SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET")
SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY")

SHOPIFY_API_VERSION = "2020-01"

REQUEST_METHODS = {
    "GET": requests.get,
    "POST": requests.post,
    "PUT": requests.put,
    "DEL": requests.delete,
}


class StoreStatus(Enum):
    NOT_KNOWN = 1
    INSTALL_REQUESTED = 2
    INSTALLED = 3
    UNINSTALLED = 4


def get_store_status(query_result: pd.DataFrame) -> StoreStatus:
    if query_result.empty:
        return StoreStatus.NOT_KNOWN
    store_data = query_result.iloc[0]
    if store_data.install_time is None:
        return StoreStatus.INSTALL_REQUESTED
    elif store_data.uninstall_time is None:
        return StoreStatus.INSTALLED
    return StoreStatus.UNINSTALLED


class PGStoreInterface:
    shop: str
    status: Optional[StoreStatus] = None
    data: Optional[pd.Series] = None

    def __init__(self, shop: str) -> None:
        self.shop = shop
        self.get_store_info(shop)

    def strip_shop_address(self, shop_address: Optional[str]) -> str:
        raw_shop_address = self.shop if shop_address is None else shop_address
        split_address = raw_shop_address.split("https://")
        return split_address[0] if len(split_address) == 1 else split_address[1]

    def get_status(self, shop: Optional[str] = None):
        self.get_store_info(shop)
        return self.status

    def get_store_info(self, shop: Optional[str] = None) -> pd.Series:
        pg_shop_address = self.strip_shop_address(shop)
        find_shop_query = f"""
            SELECT * from shopify_shopifystore WHERE shop_address='{pg_shop_address}'
        """
        query_result = get_pgsql_pandas_data(find_shop_query)
        self.status = get_store_status(query_result)
        result = pd.Series() if query_result.empty else query_result.iloc[0]
        self.data = result

        logger.info(f"Getting info for shop: {pg_shop_address}")
        logger.info(f"Status: " + str(self.status))
        logger.info(f"DB entry: " + str(result))
        return result

    def request_install(self, shop: Optional[str] = None) -> None:
        nonce = uuid.uuid4().hex
        ask_time = int(pd.Timestamp.utcnow().timestamp() * 1e9)
        pg_shop_address = self.strip_shop_address(shop)

        insert_shop_query = f"""
            INSERT INTO shopify_shopifystore(shop_address, nonce, ask_time) VALUES
            ('{pg_shop_address}', '{nonce}', {ask_time});
        """
        execute_pgsql(insert_shop_query)
        self.get_store_info()

    def request_reinstall(self, shop: Optional[str] = None) -> None:
        nonce = uuid.uuid4().hex
        ask_time = int(pd.Timestamp.utcnow().timestamp() * 1e9)
        pg_shop_address = self.strip_shop_address(shop)

        reinstall_request_query = f"""
            UPDATE shopify_shopifystore SET
            uninstall_time = NULL, install_time = NULL, ask_time = {ask_time}, nonce = {nonce}, access_token = NULL
            WHERE shop_address = '{pg_shop_address}';
        """
        execute_pgsql(reinstall_request_query)
        self.get_store_info()

    def confirm_installation(
        self, access_token: str, shop: Optional[str] = None
    ) -> None:
        pg_shop_address = self.strip_shop_address(shop)
        install_time = int(pd.Timestamp.utcnow().timestamp() * 1e9)
        confirm_install_query = f"""
                    UPDATE shopify_shopifystore SET
                    install_time = {install_time}, access_token = '{access_token}'
                    WHERE shop_address = '{pg_shop_address}';
                """
        execute_pgsql(confirm_install_query)

    def uninstall(self, shop: Optional[str] = None) -> None:
        pg_shop_address = self.strip_shop_address(shop)
        uninstall_time = int(pd.Timestamp.utcnow().timestamp() * 1e9)
        uninstall_query = f"""
                    UPDATE shopify_shopifystore SET
                    uninstall_time = {uninstall_time}
                    WHERE shop_address = '{pg_shop_address}';
                """
        execute_pgsql(uninstall_query)


class ShopifyStoreClient:
    def __init__(self, shop: str, access_token: str):
        self.shop = shop
        self.base_url = f"https://{shop}/admin/api/{SHOPIFY_API_VERSION}/"
        self.access_token = access_token

    @staticmethod
    def authenticate(shop: str, code: str) -> Optional[str]:
        url = f"https://{shop}/admin/oauth/access_token"
        payload = {
            "client_id": SHOPIFY_API_KEY,
            "client_secret": SHOPIFY_API_SECRET,
            "code": code,
        }
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json()["access_token"]
        except HTTPError as ex:
            logging.exception(ex)
            return None

    def authenticated_shopify_call(
        self,
        call_path: str,
        method: str,
        params: dict = None,
        payload: dict = None,
        headers: dict = {},
    ) -> Optional[dict]:
        url = f"{self.base_url}{call_path}"
        request_func = REQUEST_METHODS[method]
        headers["X-Shopify-Access-Token"] = self.access_token
        try:
            response = request_func(url, params=params, json=payload, headers=headers)
            response.raise_for_status()
            logging.debug(
                f"authenticated_shopify_call response:\n{json.dumps(response.json(), indent=4)}"
            )
            return response.json()
        except HTTPError as ex:
            logging.exception(ex)
            return None

    def get_shop(self) -> Optional[dict]:
        call_path = "shop.json"
        method = "GET"
        shop_response = self.authenticated_shopify_call(
            call_path=call_path, method=method
        )
        if not shop_response:
            return None
        # The myshopify_domain value is the one we'll need to listen to via webhooks to determine an uninstall
        return shop_response["shop"]

    def get_script_tags(self) -> Optional[List]:
        call_path = "script_tags.json"
        method = "GET"
        script_tags_response = self.authenticated_shopify_call(
            call_path=call_path, method=method
        )
        if not script_tags_response:
            return None
        return script_tags_response["script_tags"]

    def get_script_tag(self, id: int) -> Optional[dict]:
        call_path = f"script_tags/{id}.json"
        method = "GET"
        script_tag_response = self.authenticated_shopify_call(
            call_path=call_path, method=method
        )
        if not script_tag_response:
            return None
        return script_tag_response["script_tag"]

    def update_script_tag(
        self, id: int, src: str, display_scope: str = None
    ) -> Optional[bool]:
        call_path = f"script_tags/{id}.json"
        method = "PUT"
        payload = {"script_tag": {"id": id, "src": src}}
        if display_scope:
            payload["script_tag"]["display_scope"] = display_scope
        script_tags_response = self.authenticated_shopify_call(
            call_path=call_path, method=method, payload=payload
        )
        if not script_tags_response:
            return None
        return script_tags_response["script_tag"]

    def create_script_tag(
        self, src: str, event: str = "onload", display_scope: str = None
    ) -> Optional[int]:
        call_path = "script_tags.json"
        method = "POST"
        payload = {"script_tag": {"event": event, "src": src}}
        if display_scope:
            payload["script_tag"]["display_scope"] = display_scope
        script_tag_response = self.authenticated_shopify_call(
            call_path=call_path, method=method, payload=payload
        )
        if not script_tag_response:
            return None
        return script_tag_response["script_tag"]

    def delete_script_tag(self, script_tag_id: int) -> bool:
        call_path = f"script_tags/{script_tag_id}.json"
        method = "DEL"
        script_tag_response = self.authenticated_shopify_call(
            call_path=call_path, method=method
        )
        if script_tag_response is None:
            return False
        return True

    def create_usage_charge(
        self, recurring_application_charge_id: int, description: str, price: float
    ) -> Optional[dict]:
        call_path = f"recurring_application_charges/{recurring_application_charge_id}/usage_charges.json"
        method = "POST"
        payload = {"usage_charge": {"description": description, "price": price}}
        usage_charge_response = self.authenticated_shopify_call(
            call_path=call_path, method=method, payload=payload
        )
        if not usage_charge_response:
            return None
        return usage_charge_response["usage_charge"]

    def get_recurring_application_charges(self) -> Optional[List]:
        call_path = "recurring_application_charges.json"
        method = "GET"
        recurring_application_charges_response = self.authenticated_shopify_call(
            call_path=call_path, method=method
        )
        if not recurring_application_charges_response:
            return None
        return recurring_application_charges_response["recurring_application_charges"]

    def delete_recurring_application_charges(
        self, recurring_application_charge_id: int
    ) -> bool:
        # Broken currently,authenticated_shopify_call expects JSON but this returns nothing
        call_path = (
            f"recurring_application_charges/{recurring_application_charge_id}.json"
        )
        method = "DEL"
        delete_recurring_application_charge_response = self.authenticated_shopify_call(
            call_path=call_path, method=method
        )
        if delete_recurring_application_charge_response is None:
            return False
        return True

    def activate_recurring_application_charge(
        self, recurring_application_charge_id: int
    ) -> Optional[dict]:
        call_path = f"recurring_application_charges/{recurring_application_charge_id}/activate.json"
        method = "POST"
        payload = {}
        recurring_application_charge_activation_response = (
            self.authenticated_shopify_call(
                call_path=call_path, method=method, payload=payload
            )
        )
        if not recurring_application_charge_activation_response:
            return None
        return recurring_application_charge_activation_response[
            "recurring_application_charge"
        ]

    def create_webook(self, address: str, topic: str) -> Optional[dict]:
        call_path = "webhooks.json"
        method = "POST"
        payload = {"webhook": {"topic": topic, "address": address, "format": "json"}}
        webhook_response = self.authenticated_shopify_call(
            call_path=call_path, method=method, payload=payload
        )
        if not webhook_response:
            return None
        return webhook_response["webhook"]

    def get_webhooks_count(self, topic: str):
        call_path = f"webhooks/count.json?topic={topic}"
        method = "GET"
        webhook_count_response = self.authenticated_shopify_call(
            call_path=call_path, method=method
        )
        if not webhook_count_response:
            return None
        return webhook_count_response["count"]
