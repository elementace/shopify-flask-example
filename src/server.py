import json
import logging
import os
from typing import Any, Dict, Tuple, Union

import sentry_sdk
from flask import Flask, redirect, request
from sentry_sdk.integrations.flask import FlaskIntegration

from .client_app import client_side_app
from .helpers import (SERVER_BASE_URL, generate_install_redirect_url,
                      generate_post_install_redirect_url,
                      post_message_to_slack, verify_web_call,
                      verify_webhook_call, webhook_fail)
from .shopify_client import PGStoreInterface, ShopifyStoreClient, StoreStatus
from .shopify_interpreter import create_prds_on_shopify, find_discount
from .shopify_munging import process_order

logger = logging.getLogger(__name__)
sentry_sdk.init(
    dsn="https://3b9ea6a8c76e4ff997674020c6c596e2@o547884.ingest.sentry.io/5749865",
    integrations=[FlaskIntegration()],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0,
)

app = Flask(__name__)

ACCESS_MODE = []  # Defaults to offline access mode if left blank or omitted.
# https://shopify.dev/docs/admin-api/access-scopes
SCOPES = [
    "write_price_rules",
    "write_script_tags",
    "write_discounts",
    "read_orders",
    "read_products",
    "read_customers",
]
WEBHOOKS = {
    "app/uninstalled": f"{SERVER_BASE_URL}/app_uninstalled",
    "orders/create": f"{SERVER_BASE_URL}/order_created",
    "refunds/create": f"{SERVER_BASE_URL}/refund_created",
    # "customers/redact": f"{SERVER_BASE_URL}/redact_customers",
    # "shop/redact": f"{SERVER_BASE_URL}/redact_shop",
    # "customers/data_request": f"{SERVER_BASE_URL}/customers_data_request",
}
# https://shopify.dev/tutorials/add-gdpr-webhooks-to-your-app#customers-data_request


@app.route("/", methods=["GET"])
def landing():
    return """
    <h1>Gooie Shopify App</h1>
    </br>Welcome to the Gooie app for Shopify, please login to Shopify
     and use the Shopify specific link to install this App
    """


@app.route("/app_launched", methods=["GET"])
@verify_web_call
def app_launched():
    logger.info("app_launched hit")
    logger.info(request.get_json())
    shop = request.args.get("shop")
    store_db_interface = PGStoreInterface(shop=shop)
    shop_status = store_db_interface.status

    if shop_status == StoreStatus.NOT_KNOWN:
        store_db_interface.request_install(shop=shop)
    elif shop_status == StoreStatus.UNINSTALLED:
        store_db_interface.request_reinstall(shop=shop)
    elif shop_status == StoreStatus.INSTALLED:
        if store_db_interface.data["needs_rescope"]:
            store_db_interface.request_reinstall(shop=shop)
        else:
            return client_side_app(store_db_interface)
    else:
        return (
            f"""
            You've navigated to the install app stage,
            but this app is already installed on {shop}
            """,
            400,
        )

    nonce = store_db_interface.data["nonce"]
    redirect_url = generate_install_redirect_url(
        shop=shop, scopes=SCOPES, nonce=nonce, access_mode=ACCESS_MODE
    )
    return redirect(redirect_url, code=302)


@app.route("/app_installed", methods=["GET"])
@verify_web_call
def app_installed():
    logger.info("app_installed hit")
    logger.info(request.get_json())
    state = request.args.get("state")
    shop = request.args.get("shop")
    code = request.args.get("code")
    store_db_interface = PGStoreInterface(shop=shop)

    if store_db_interface.status != StoreStatus.INSTALL_REQUESTED:
        return (
            f"""
            You've navigated to the installation confirmation stage,
             but this shop: {shop}
             has not recently requested install
        """,
            400,
        )

    if state != store_db_interface.data["nonce"]:
        return "Invalid `state` received, cannot confirm installation", 400

    ACCESS_TOKEN = ShopifyStoreClient.authenticate(shop=shop, code=code)
    store_db_interface.confirm_installation(ACCESS_TOKEN)
    post_message_to_slack(
        f"""
        App Installed for {store_db_interface.shop}
    """
    )
    shopify_client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)

    _ = [
        shopify_client.create_webook(address, topic)
        for topic, address in WEBHOOKS.items()
    ]

    rac_response = shopify_client.create_recurring_application_charges()
    if rac_response is None:
        logger.error("RAC didnt return a response, can't set rac_id")
    else:
        store_db_interface.update_rac_id(rac_response.get("id"))
        post_message_to_slack(
            f"""
                Recurring Charges set up for {store_db_interface.shop}
            """
        )

    redirect_url = generate_post_install_redirect_url(shop=shop)
    return redirect(redirect_url, code=302)


def handle_webhook(
    request: Any, webhook_name: str
) -> Union[str, Tuple[str, Dict, PGStoreInterface]]:
    logger.info(f"{webhook_name} hit")
    webhook_topic = request.headers.get("X-Shopify-Topic")
    webhook_payload = request.get_json()
    shop_address = request.headers.get("X-Shopify-Shop-Domain") or webhook_payload.get(
        "domain"
    )
    logger.info(request.get_json())

    if shop_address is None:
        return webhook_fail(
            webhook_topic, webhook_payload, error_str="No shop found in header"
        )
    store_db_interface = PGStoreInterface(shop=shop_address)
    if not store_db_interface.status == StoreStatus.INSTALLED:
        error_str = f"Shopify Store {shop_address} not installed. Status: {store_db_interface.status}"
        return webhook_fail(webhook_topic, webhook_payload, error_str=error_str)

    return webhook_topic, webhook_payload, store_db_interface


@app.route("/app_uninstalled", methods=["POST"])
@verify_webhook_call
def app_uninstalled():
    webhook_values = handle_webhook(request, "app_uninstalled")
    if isinstance(webhook_values, str):
        return webhook_values
    webhook_topic, webhook_payload, sDBI = webhook_values

    logger.info(f"Uninstalling {sDBI.shop}")
    sDBI.uninstall()
    logger.info("Uninstall Successful")

    post_message_to_slack(
        f"""
        App uninstalled @ {sDBI.shop}
    """
    )
    return f"Gooie App uninstalled for {sDBI.shop}"


@app.route("/redact_customers", methods=["POST"])
@verify_webhook_call
def redact_customers():
    webhook_values = handle_webhook(request, "redact_customers")
    if isinstance(webhook_values, str):
        return webhook_values
    webhook_topic, webhook_payload, sDBI = webhook_values

    post_message_to_slack(
        f"""
        redact_customers @ {sDBI.shop}:\n{json.dumps(webhook_payload, indent=4)}
    """
    )

    return "OK"


@app.route("/redact_shop", methods=["POST"])
@verify_webhook_call
def redact_shop():
    webhook_values = handle_webhook(request, "redact_shop")
    if isinstance(webhook_values, str):
        return webhook_values
    webhook_topic, webhook_payload, sDBI = webhook_values

    post_message_to_slack(
        f"""
        redact_shop @ {sDBI.shop}:\n{json.dumps(webhook_payload, indent=4)}
    """
    )

    return "OK"


@app.route("/customers_data_request", methods=["POST"])
@verify_webhook_call
def customers_data_request():
    webhook_values = handle_webhook(request, "customers_data_request")
    if isinstance(webhook_values, str):
        return webhook_values
    webhook_topic, webhook_payload, sDBI = webhook_values

    post_message_to_slack(
        f"""
        customers_data_request @ {sDBI.shop}:\n{json.dumps(webhook_payload, indent=4)}
    """
    )
    return "OK"


@app.route("/order_created", methods=["POST"])
@verify_webhook_call
def order_created():
    webhook_values = handle_webhook(request, "order_created")
    if isinstance(webhook_values, str):
        return webhook_values
    webhook_topic, order, sDBI = webhook_values
    post_message_to_slack(
        f"""
        order_created @ {sDBI.shop}:\n{json.dumps(order, indent=4)}
    """,
        "#new_orders",
    )
    process_order(sDBI, order)
    return "OK"


@app.route("/refund_created", methods=["POST"])
@verify_webhook_call
def refund_created():
    webhook_values = handle_webhook(request, "refund_created")
    if isinstance(webhook_values, str):
        return webhook_values
    webhook_topic, webhook_payload, sDBI = webhook_values
    post_message_to_slack(
        f"""
        refund_created @ {sDBI.shop}:\n{json.dumps(webhook_payload, indent=4)}
    """,
        "#new_refunds",
    )
    return "OK"


@app.route("/create_discount", methods=["GET"])
def create_discount():
    logger.info("/create_discount hit")
    logger.info(request.headers)
    logger.info(request.json())
    logger.info(request.__dict__)
    request_args = request.get_json()
    logger.info(request_args)
    discount_code = request_args.get("discount_name")
    prd_obj = find_discount(discount_code)
    if prd_obj is None:
        return f"Couldn't find discount_code {discount_code} in database"
    create_prds_on_shopify(prd_obj)
    return "OK"


if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
