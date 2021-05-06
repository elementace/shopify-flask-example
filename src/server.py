import json
import logging
import os

import sentry_sdk
from flask import Flask, redirect, request
from sentry_sdk.integrations.flask import FlaskIntegration

from .helpers import (WEBHOOK_APP_UNINSTALL_URL, generate_install_redirect_url,
                      generate_post_install_redirect_url, verify_web_call,
                      verify_webhook_call)
from .shopify_client import PGStoreInterface, ShopifyStoreClient, StoreStatus

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
SCOPES = ["write_script_tags", "write_customers", "write_discounts", "read_orders"]


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
    shop = request.args.get("shop")
    store_db_interface = PGStoreInterface(shop=shop)
    shop_status = store_db_interface.status

    if shop_status == StoreStatus.NOT_KNOWN:
        store_db_interface.request_install(shop=shop)
    elif shop_status == StoreStatus.UNINSTALLED:
        store_db_interface.request_reinstall(shop=shop)
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

    shopify_client = ShopifyStoreClient(shop=shop, access_token=ACCESS_TOKEN)
    shopify_client.create_webook(
        address=WEBHOOK_APP_UNINSTALL_URL, topic="app/uninstalled"
    )

    redirect_url = generate_post_install_redirect_url(shop=shop)
    return redirect(redirect_url, code=302)


@app.route("/app_uninstalled", methods=["POST"])
@verify_webhook_call
def app_uninstalled():
    # https://shopify.dev/docs/admin-api/rest/reference/events/webhook?api[version]=2020-04
    # Someone uninstalled your app, clean up anything you need to

    webhook_topic = request.headers.get("X-Shopify-Topic")
    webhook_payload = request.get_json()
    logging.error(
        f"Uninstall webhook call received {webhook_topic}:\n{json.dumps(webhook_payload, indent=4)}"
    )
    return "OK"


@app.route("/data_removal_request", methods=["POST"])
@verify_webhook_call
def data_removal_request():
    # https://shopify.dev/tutorials/add-gdpr-webhooks-to-your-app
    # Clear all personal information you may have stored about the specified shop
    return "OK"


@app.route("/debug-sentry")
def trigger_error():
    division_by_zero = 1 / 0
    return division_by_zero


if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
