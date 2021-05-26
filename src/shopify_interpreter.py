import logging
from typing import Optional

import pandas as pd

from .database import execute_pgsql, get_pgsql_pandas_data
from .shopify_client import PGStoreInterface

logger = logging.getLogger(__name__)


def find_discount(discount_name: Optional[str]) -> Optional[pd.Series]:
    if discount_name is None:
        return None
    discount_query = f"""
    SELECT * FROM shopify_pricerulediscount WHERE discount_code = '{discount_name}'
    """
    discount = get_pgsql_pandas_data(discount_query)
    return discount.iloc[0]


def get_business_by_id(business_id: int) -> pd.Series:
    business_query = f"""
    SELECT * FROM businesses_business
    WHERE id={business_id}
    """
    res = get_pgsql_pandas_data(business_query)
    return res.iloc[0]


def get_business_by_shopify_address(shopify_address: str) -> pd.Series:
    business_query = f"""
    SELECT * FROM businesses_business
    WHERE shopify_address='{shopify_address}'
    """
    res = get_pgsql_pandas_data(business_query)
    return res.iloc[0]


def get_user_by_id(user_id: int) -> pd.Series:
    user_query = f"""
    SELECT * FROM users_user
    WHERE id={user_id}
    """
    res = get_pgsql_pandas_data(user_query)
    return res.iloc[0]


def get_shopify_store_from_discount(prd_obj: pd.Series) -> str:
    business_srs = get_business_by_id(prd_obj.business_id)
    return business_srs.shopify_address


def get_discount_config(business_id: int) -> pd.Series:
    user_query = f"""
        SELECT * FROM businesses_discountconfiguration
        WHERE business_id={business_id}
        """
    res = get_pgsql_pandas_data(user_query)
    return res.iloc[0]


def assign_prd_ids(discount_name, price_rule_id, discount_id):
    set_prd_ids_query = f"""
        UPDATE shopify_pricerulediscount
        SET price_rule_id = {price_rule_id}, discount_id={discount_id}
        WHERE discount_code = '{discount_name}';
    """
    execute_pgsql(set_prd_ids_query)


def convert_prd(discount_name):
    convert_prd_query = f"""
        UPDATE shopify_pricerulediscount
        SET converted = TRUE
        WHERE discount_code = '{discount_name}';
    """
    execute_pgsql(convert_prd_query)


def create_prds_on_shopify(prd_obj: pd.Series):
    shop = get_shopify_store_from_discount(prd_obj)
    store_db_interface = PGStoreInterface(shop=shop)
    shopify_client = store_db_interface.generate_shopify_client()
    pr_response = shopify_client.create_price_rule(
        discount_level=prd_obj.discount,
        discount_name=prd_obj["discount_code"],
        start_time=prd_obj.start_time,
        end_time=prd_obj.end_time,
    )
    logger.info(f"price_rule created: {pr_response}")
    discount_response = shopify_client.create_discount_code(
        price_rule_id=pr_response["id"], discount_name=prd_obj["discount_code"]
    )
    logger.info(f"discount created: {discount_response}")
    assign_prd_ids(prd_obj["discount_code"], pr_response["id"], discount_response["id"])
