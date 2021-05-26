import logging
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from .database import execute_pgsql, get_pgsql_pandas_data
from .helpers import post_message_to_slack
from .shopify_client import PGStoreInterface, ShopifyStoreClient
from .shopify_interpreter import (convert_prd, find_discount,
                                  get_business_by_shopify_address,
                                  get_discount_config)

logger = logging.getLogger(__name__)

INSERT_QUERY = """
INSERT INTO {tbl}({cols}) VALUES ({values});
"""

SELECT_QUERY = """
SELECT * FROM {tbl} WHERE {id_field} = {id_value}
"""

UPDATE_QUERY = """
UPDATE {tbl} SET {equations} WHERE {id_field} = {id_value}
"""


def maybe_number_str(val: Any, is_field_name: bool) -> str:
    try:
        ival = int(val)
        if int(val) == float(val):
            return str(ival)
        else:
            return str(round(float(val), 2))
    except:
        try:
            isoformat = pd.Timestamp(val).tz_convert("utc").isoformat() + "Z"
            return f"'{isoformat}'"
        except:
            if not is_field_name:
                return f"'{val}'"
            else:
                return val


def build_sql_str(value: Any, is_field_name: bool = False) -> str:
    if value == "now()":
        return value
    if isinstance(value, str):
        return maybe_number_str(value, is_field_name)
    elif value is None:
        return "NULL"
    elif isinstance(value, bool):
        return str(value).upper()
    elif isinstance(value, int):
        return str(value)
    elif isinstance(value, float):
        return str(round(value, 2))
    else:
        logger.error(
            f"got {value} of type: {type(value)} and didnt haven't an option for it"
        )
        raise TypeError


def build_update_list(value_dict: Dict) -> str:
    return ", ".join([f"{k} = {build_sql_str(v, )}" for k, v in value_dict.items()])


def build_sql_list(value_list: Iterable, is_field_name: bool = False) -> str:
    return ", ".join([build_sql_str(val, is_field_name) for val in value_list])


def insert_model(table_name: str, data: dict):
    insert_model_query = INSERT_QUERY.format(
        tbl=table_name,
        cols=build_sql_list(data.keys(), is_field_name=True),
        values=build_sql_list(data.values()),
    )
    execute_pgsql(insert_model_query)


def update_model(table_name: str, data: dict, old_data: dict, id_field: str) -> None:
    data_to_update = {
        k: data[k] for k, old_value in old_data.items() if data[k] != old_value
    }
    if len(data_to_update) == 0:
        return
    update_model_query = UPDATE_QUERY.format(
        tbl=table_name,
        equations=build_update_list(data_to_update),
        id_field=id_field,
        id_value=old_data[id_field],
    )
    execute_pgsql(update_model_query)


def check_exists(table_name: str, id_field: str, id_value: int) -> Optional[Dict]:
    if any([x is None for x in (table_name, id_field, id_value)]):
        return
    exists_query = SELECT_QUERY.format(
        tbl=table_name, id_field=id_field, id_value=id_value
    )
    query_df = get_pgsql_pandas_data(exists_query)
    if query_df.empty:
        return None
    return query_df.iloc[0].to_dict()


def load_json_to_database(
    table_name: str,
    processed_data: Dict,
    id_field: Optional[str] = None,
    select: bool = False,
):
    id_value = None if id_field is None else processed_data[id_field]
    data_exists = (
        None if id_field is None else check_exists(table_name, id_field, id_value)
    )
    if data_exists is None:
        insert_model(table_name, processed_data)
    else:
        update_model(table_name, processed_data, data_exists, id_field)
    if select:
        return check_exists(table_name, id_field, id_value)
    return None


def munge_address(order_address_json: Dict) -> Dict:
    data = order_address_json.copy()
    data["address_id"] = data["id"]
    data["is_default"] = data["default"]
    deleted_entries = ["id", "default", "customer_id", "company", "name"]
    for entry in deleted_entries:
        del data[entry]
    return data


def munge_customer(order_customer_data: Dict, shopify_store_address: str) -> Dict:
    data = order_customer_data.copy()
    data["customer_id"] = data["id"]
    data["shopify_store"] = shopify_store_address
    data["default_address_id"] = data["default_address"]["id"]
    data["total_spent"] = float()
    deleted_entries = [
        "id",
        "default_address",
        "last_order_id",
        "phone",
        "last_order_name",
        "accepts_marketing_updated_at",
        "marketing_opt_in_level",
        "tax_exempt",
        "tax_exemptions",
        "admin_graphql_api_id",
    ]
    for entry in deleted_entries:
        del data[entry]
    return data


def munge_referral_data(order_data: Dict) -> Dict:
    fields = [
        "landing_site",
        "landing_site_ref",
        "referring_site",
        "source_identifier",
        "source_name",
        "source_url",
    ]
    data = {k: order_data[k] for k in fields}
    data["order_id"] = order_data["id"]
    return data


def munge_line_item(order_line_item_data, order_id):
    data = order_line_item_data.copy()
    data["order_id"] = order_id
    data["line_item_id"] = data["id"]
    data["product_name"] = data["name"]
    data.update(access_amount_set(data, "price", "price_set"))
    deleted_entries = [
        "id",
        "admin_graphql_api_id",
        "destination_location",
        "fulfillable_quantity",
        "fulfillment_service",
        "fulfillment_status",
        "grams",
        "origin_location",
        "properties",
        "total_discount_set",
        "variant_inventory_management",
        "tax_lines",
        "duties",
        "price_set",
        "discount_allocations",
        "name",
        "price",
        "total_discount",
    ]
    for entry in deleted_entries:
        del data[entry]

    data["created_at"] = str(pd.Timestamp.utcnow())
    data["updated_at"] = str(pd.Timestamp.utcnow())
    return data


def access_amount_set(data_set, db_prefix, set_name):
    presentment = data_set[set_name]["presentment_money"]
    shop = data_set[set_name]["shop_money"]
    data = {
        f"{db_prefix}_presentment": presentment["amount"],
        f"{db_prefix}_presentment_currency": presentment["currency_code"],
        f"{db_prefix}_shop": shop["amount"],
        f"{db_prefix}_shop_currency": shop["currency_code"],
    }
    return data


def munge_line_item_discount(li_discount_data, line_item_id):
    data = {
        "discount_application_index": li_discount_data["discount_application_index"],
        "line_item_id": line_item_id,
    }
    data.update(access_amount_set(li_discount_data, "discount", "amount_set"))
    return data


def munge_order(order_data, business_id):
    data = {
        "order_id": order_data["id"],
        "business_id": business_id,
        "shop_order_number": order_data["order_number"],
        "customer_id": order_data["customer"]["id"],
        "pos_location_id": order_data["location_id"],
        "pos_user_id": order_data["user_id"],
    }
    copied_keys = [
        "note",
        "tags",
        "test",
        "financial_status",
        "cancel_reason",
        "cancelled_at",
        "closed_at",
        "taxes_included",
    ]
    for key in copied_keys:
        data[key] = order_data[key]

    data.update(
        **{
            **access_amount_set(
                order_data, "total_line_items", "total_line_items_price_set"
            ),
            **access_amount_set(order_data, "total_price", "total_price_set"),
            **access_amount_set(
                order_data, "total_shipping", "total_shipping_price_set"
            ),
            **access_amount_set(order_data, "total_tax", "total_tax_set"),
        }
    )

    data["created_at"] = str(pd.Timestamp.utcnow())
    data["updated_at"] = str(pd.Timestamp.utcnow())
    return data


def munge_applied_discounts(discount_data, order_id, item_rank):
    data = discount_data.copy()
    print(data)
    data["order_id"] = order_id
    data["discount_code"] = data["code"]
    data["applied_discount_number"] = item_rank
    del data["code"]
    return data


def load_data_to_db(order, shopify_store_address, business_id):
    load_json_to_database(
        table_name="shopify_address",
        processed_data=munge_address(order["customer"]["default_address"]),
        id_field="address_id",
    )
    load_json_to_database(
        table_name="shopify_customer",
        processed_data=munge_customer(order["customer"], shopify_store_address),
        id_field="customer_id",
    )

    load_json_to_database(
        table_name="shopify_order",
        processed_data=munge_order(order, business_id),
        id_field="order_id",
    )
    for i, applied_discounts in enumerate(order["discount_codes"]):
        load_json_to_database(
            table_name="shopify_appliedorderdiscounts",
            processed_data=munge_applied_discounts(applied_discounts, order["id"], i),
        )

    for line_item in order["line_items"]:
        load_json_to_database(
            table_name="shopify_lineitem",
            processed_data=munge_line_item(line_item, order["id"]),
            id_field="line_item_id",
        )
        for discount_data in line_item["discount_allocations"]:
            load_json_to_database(
                table_name="shopify_lineitemdiscounts",
                processed_data=munge_line_item_discount(discount_data, line_item["id"]),
            )
    load_json_to_database(
        table_name="shopify_orderreferraldata",
        processed_data=munge_referral_data(order),
    )


def create_kickback(
    order: Dict,
    discount_config: pd.Series,
    prd_obj: pd.Series,
    matched_discount: pd.Series,
):
    # TODO: Add Gooie fee % logic
    applied_discounts = {d["code"]: d for d in order["discount_codes"]}
    discount_obj = applied_discounts[matched_discount]
    total_item_purchase = order["total_line_items_price_set"]["shop_money"]

    kickback_units = (
        float(total_item_purchase["amount"])
        * discount_config.endorser_kickback_percent
        / 100.0
    )
    kickback = {
        "created_at": "now()",
        "updated_at": "now()",
        "business_id": int(prd_obj["business_id"]),
        "endorser_id": prd_obj["endorser_id"],
        "prd_id": prd_obj["discount_code"],
        "order_id": order["id"],
        "kickback": kickback_units,
        "kickback_currency": total_item_purchase["currency_code"],
        "discount": round(float(discount_obj["amount"]), 2),
        "discount_currency": order["currency"],
        "paid_out_percent": 0.0,
        "gooie_fee_percent": 30.0,
    }
    insert_model("endorser_kickback", kickback)
    return kickback


def get_usd_amount(amount, shop_currency):
    currency_rate = {"AUD": 0.78}
    return amount * currency_rate[shop_currency] * 1.003


def create_usage_charge(
    rac_id, shopify_client: ShopifyStoreClient, kickback: Dict
) -> None:
    amount = kickback["kickback_units"]
    shop_currency = kickback["kickback_currency"]
    price = get_usd_amount(amount, shop_currency)
    shopify_client.create_usage_charge(
        recurring_application_charge_id=rac_id,
        description=f'Gooie charge on order: {kickback["order_id"]} unique discount: {kickback["prd_id"]}',
        price=price,
    )
    post_message_to_slack(
        f"""
            Created Charge of {price} for {shopify_client.shop}:\n
        """,
        "#new_refunds",
    )


def handle_matched_discount(
    order: Dict,
    business: pd.Series,
    sDBI: PGStoreInterface,
    possible_discount_matches: Optional[Dict],
) -> None:
    discount_config = get_discount_config(business.id)
    matched_discount = list(possible_discount_matches.keys())[0]
    prd_obj = possible_discount_matches[matched_discount]
    kickback = create_kickback(order, discount_config, prd_obj, matched_discount)
    shopify_client = sDBI.generate_shopify_client()
    if sDBI.data.rac_id is not None:
        create_usage_charge(sDBI.data.rac_id, shopify_client, kickback)
    convert_prd(prd_obj.discount_name)
    shopify_client.delete_price_rule(prd_obj.price_rule_id)
    shopify_client.delete_discount_code(prd_obj.price_rule_id, prd_obj.discount_id)


def process_order(sDBI, order):
    business = get_business_by_shopify_address(sDBI.shop)
    load_data_to_db(order, sDBI.shop, business["id"])
    possible_discount_matches = {
        d["code"]: find_discount(d["code"]) for d in order["discount_codes"]
    }
    if len(possible_discount_matches) > 0:
        handle_matched_discount(order, business, sDBI, possible_discount_matches)
