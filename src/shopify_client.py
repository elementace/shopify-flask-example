import json
import logging
import os
import uuid
from enum import Enum
from typing import Dict, List, Optional

import pandas as pd
import requests
from requests.exceptions import HTTPError

from .database import execute_pgsql, get_pgsql_pandas_data

logger = logging.getLogger(__name__)

SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET")
SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY")
POST_RECURRING_CHARGE_URL = os.environ.get("POST_RECURRING_CHARGE_URL")
SHOPIFY_API_VERSION = "2021-04"

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
            logger.exception(ex)
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
            if response.content:
                logger.debug(
                    f"authenticated_shopify_call response:\n{json.dumps(response.json(), indent=4)}"
                )
                return response.json()
            return None
        except HTTPError as ex:
            logger.exception(ex)
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

    def create_discount_code(
        self, price_rule_id: str, discount_name: str
    ) -> Optional[Dict]:
        call_path = f"price_rules/{price_rule_id}/discount_codes.json"
        method = "POST"
        payload = {"discount_rule": {"code": f"{discount_name}"}}
        discount_response = self.authenticated_shopify_call(
            call_path=call_path, method=method, payload=payload
        )
        if not discount_response:
            return None
        return discount_response["discount_code"]

    def delete_discount_code(self, price_rule_id, discount_id):
        call_path = f"price_rules/{price_rule_id}/discount_codes/{discount_id}.json"
        method = "DEL"
        delete_discount_code_response = self.authenticated_shopify_call(
            call_path=call_path, method=method
        )
        if delete_discount_code_response is None:
            return False
        return True

    def create_price_rule(
        self,
        discount_level: str,
        discount_name: str,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        customer_selection: str = "all",
        target_selection: str = "all",
    ) -> Optional[Dict]:
        call_path = "price_rules.json"
        method = "POST"
        payload = {
            "price_rule": {
                "value_type": "percentage",
                "value": f"-{discount_level}",
                "customer_selection": customer_selection,  # 'prerequisite',
                # 'prerequisite_customer_ids': [384028349005],
                "target_type": "line_item",
                "target_selection": target_selection,  # 'entitled',
                # 'entitled_collection_ids': [ 4564654869, 979761006 ]
                # 'entitled_product_ids': [ 4564654869, 979761006 ],
                "once_per_customer": "true",
                "allocation_method": "across",
                "starts_at": start_time.isoformat() + "Z",
                "ends_at": end_time.isoformat() + "Z",
                "title": discount_name,
            }
        }
        price_rule_response = self.authenticated_shopify_call(
            call_path=call_path, method=method, payload=payload
        )
        if not price_rule_response:
            return None
        return price_rule_response["price_rule"]

    def delete_price_rule(self, price_rule_id):
        call_path = f"price_rules/{price_rule_id}.json"
        method = "DEL"
        delete_price_rule_response = self.authenticated_shopify_call(
            call_path=call_path, method=method
        )
        if delete_price_rule_response is None:
            return False
        return True

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

    def create_recurring_application_charges(self) -> Optional[Dict]:
        call_path = "recurring_application_charges.json"
        method = "POST"
        payload = {
            "recurring_application_charge": {
                "name": "Gooie Endorser Reward Program",
                "price": 0.0,
                "return_url": POST_RECURRING_CHARGE_URL,
                "capped_amount": 1000.0,
                "terms": "Pay % rewards to endorsers for converted sales",
            }
        }
        logger.info(f"Establishing recurring_application_charge for {self.shop}")
        recurring_application_charges_response = self.authenticated_shopify_call(
            call_path=call_path, method=method, payload=payload
        )
        if not recurring_application_charges_response:
            logger.error("recurring_application_charges_response returned nothing")
            return None
        logger.info(
            f"Recurring app charges created, response: {recurring_application_charges_response}"
        )
        return recurring_application_charges_response["recurring_application_charge"]

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
        logger.info("creating webhook")
        logger.info(f"payload: {payload}")
        logger.info(f"call_path: {call_path}")
        webhook_response = self.authenticated_shopify_call(
            call_path=call_path, method=method, payload=payload
        )
        logger.info(f"webhook response {webhook_response}")
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


class PGStoreInterface:
    shop: str
    status: Optional[StoreStatus] = None
    data: Optional[pd.Series] = None

    def __init__(self, shop: str) -> None:
        self.shop = shop
        self.get_store_info(shop)

    def generate_shopify_client(
        self, shop_address: Optional[str] = None
    ) -> ShopifyStoreClient:
        raw_shop_address = self.shop if shop_address is None else shop_address
        return ShopifyStoreClient(
            shop=raw_shop_address, access_token=self.data["access_token"]
        )

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
        logger.info("Status: " + str(self.status))
        logger.info("DB entry: " + str(result))
        return result

    def request_install(self, shop: Optional[str] = None) -> None:
        nonce = uuid.uuid4().hex
        ask_time = int(pd.Timestamp.utcnow().timestamp() * 1e9)
        pg_shop_address = self.strip_shop_address(shop)

        insert_shop_query = f"""
            INSERT INTO shopify_shopifystore(shop_address, nonce, ask_time, needs_rescope) VALUES
            ('{pg_shop_address}', '{nonce}', {ask_time}, FALSE);
        """
        execute_pgsql(insert_shop_query)
        self.get_store_info()

    def request_reinstall(self, shop: Optional[str] = None) -> None:
        nonce = uuid.uuid4().hex
        ask_time = int(pd.Timestamp.utcnow().timestamp() * 1e9)
        pg_shop_address = self.strip_shop_address(shop)

        reinstall_request_query = f"""
            UPDATE shopify_shopifystore SET
            uninstall_time = NULL, install_time = NULL, ask_time = {ask_time},
            nonce = '{nonce}', access_token = NULL, needs_rescope = FALSE, rac_id = NULL
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

    def update_rac_id(self, rac_id: int, shop: Optional[str] = None):
        pg_shop_address = self.strip_shop_address(shop)
        set_rac_id_query = f"""
                            UPDATE shopify_shopifystore SET rac_id = {rac_id}
                            WHERE shop_address = '{pg_shop_address}';
                            """
        execute_pgsql(set_rac_id_query)

    def uninstall(self, shop: Optional[str] = None) -> None:
        pg_shop_address = self.strip_shop_address(shop)
        uninstall_time = int(pd.Timestamp.utcnow().timestamp() * 1e9)
        uninstall_query = f"""
                    UPDATE shopify_shopifystore SET
                    uninstall_time = {uninstall_time}, access_token = NULL, rac_id = NULL
                    WHERE shop_address = '{pg_shop_address}';
                """
        execute_pgsql(uninstall_query)
