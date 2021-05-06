import base64
import hashlib
import hmac
import logging
import os
import re
from functools import wraps
from typing import List

from flask import abort, request

logger = logging.getLogger(__name__)

SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET")
SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY")
SERVER_DOMAIN = os.environ.get("SERVER_DOMAIN")
SERVER_BASE_URL = f"https://{SERVER_DOMAIN}"
INSTALL_REDIRECT_URL = f"{SERVER_BASE_URL}/app_installed"
WEBHOOK_APP_UNINSTALL_URL = f"{SERVER_BASE_URL}/app_uninstalled"
APP_NAME = os.environ.get("APP_NAME")


def generate_install_redirect_url(
    shop: str, scopes: List, nonce: str, access_mode: List
):
    scopes_string = ",".join(scopes)
    access_mode_string = ",".join(access_mode)
    redirect_url = f"https://{shop}/admin/oauth/authorize?client_id={SHOPIFY_API_KEY}&scope={scopes_string}&redirect_uri={INSTALL_REDIRECT_URL}&state={nonce}&grant_options[]={access_mode_string}"
    logger.info(f"New shop installing {shop}")
    logger.info("redirect_url: ")
    logger.info(redirect_url)
    return redirect_url


def generate_post_install_redirect_url(shop: str):
    redirect_url = f"https://{shop}/admin/apps/{APP_NAME}"
    return redirect_url


def verify_web_call(f):
    @wraps(f)
    def wrapper(*args, **kwargs) -> bool:
        get_args = request.args
        hmac = get_args.get("hmac")
        sorted(get_args)
        data = "&".join(
            [f"{key}={value}" for key, value in get_args.items() if key != "hmac"]
        ).encode("utf-8")
        if not verify_hmac(data, hmac):
            logging.error(f"HMAC could not be verified: \n\thmac {hmac}\n\tdata {data}")
            abort(400)

        shop = get_args.get("shop")
        if shop and not is_valid_shop(shop):
            logging.error(f"Shop name received is invalid: \n\tshop {shop}")
            abort(401)
        return f(*args, **kwargs)

    return wrapper


def verify_webhook_call(f):
    @wraps(f)
    def wrapper(*args, **kwargs) -> bool:
        encoded_hmac = request.headers.get("X-Shopify-Hmac-Sha256")
        hmac = base64.b64decode(encoded_hmac).hex()

        data = request.get_data()
        if not verify_hmac(data, hmac):
            logging.error(f"HMAC could not be verified: \n\thmac {hmac}\n\tdata {data}")
            abort(401)
        return f(*args, **kwargs)

    return wrapper


def verify_hmac(data: bytes, orig_hmac: str):
    new_hmac = hmac.new(SHOPIFY_API_SECRET.encode("utf-8"), data, hashlib.sha256)
    return new_hmac.hexdigest() == orig_hmac


def is_valid_shop(shop: str) -> bool:
    # Shopify docs give regex with protocol required, but shop never includes protocol
    shopname_regex = r"[a-zA-Z0-9][a-zA-Z0-9\-]*\.myshopify\.com[\/]?"
    return re.match(shopname_regex, shop)
