# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import os
import hashlib
import random
import datetime
import json
from typing import List, Any, Tuple, Dict
from faker import Faker

from pyspark.sql.dataframe import DataFrame


def customer_name_and_id(fk_obj: Faker) -> (str, str):
    name = fk_obj.unique.name()
    uid = uid_from_string(name)
    return name, uid


def product_name_and_id(fk_obj: Faker) -> (str, str):
    part1 = fk_obj.word(part_of_speech='adjective')
    part2 = fk_obj.word(ext_word_list=[
        'kajigger', 'thing', 'doodad', 'machine', 'generator', 
        'box', 'cover', 'phone', 'monitor', 'inducer', 'hub', 'apparatus',
        'visualizer', 'radiator'
    ])
    chance = random.randint(0, 99)
    if chance < 33:
        sep = '-'
    else:
        sep = ' '

    if chance < 20:
        part0 = fk_obj.word(part_of_speech='adjective')
    else:
        part0 = None

    name = part1+sep+part2
    if part0:
        name = f'{part0} {name}'

    uid = uid_from_string(name)

    return name, uid


def generate_customer_record(fk_obj: Faker) -> List[Tuple[str, str, str]]:
    name, uid = customer_name_and_id(fk_obj)
    return [name, uid, fk_obj.city()]


def generate_product_record(fk_obj: Faker) -> List[Tuple[str, str, float]]:
    name, uid = product_name_and_id(fk_obj)
    price = random.gammavariate(6, 5)
    return [name, uid, price]


def uid_from_string(input_string: str, length: int = 12) -> str:
    return hashlib.blake2b(key=input_string.encode('utf-8'), digest_size=length).hexdigest()


def create_customer_table(faker_obj: Faker, n_customers: int = 100) -> DataFrame:
    cols = ['name', 'customer_id', 'city']
    data = [generate_customer_record(faker_obj) for _ in range(n_customers)]
    return spark.createDataFrame(data, cols)


def create_products_table(faker_obj: Faker, n_products: int = 1000) -> DataFrame:
    cols = ['product_name', 'product_id', 'cost']
    data = [generate_product_record(faker_obj) for _ in range(n_products)]
    return spark.createDataFrame(data, cols)


def create_pos_table(
        faker_obj: Faker, customer_ids: List[str], product_ids: List[str],
        n_lines_min: int = 1, n_lines_max: int = 10, n_baskets: int = 500, max_amount_per_line: int = 10,
        date_min: datetime.datetime = None, date_max: datetime.datetime = None) -> DataFrame:
    cols = ['customer_id', 'product_id', 'amount', 'purchase_datetime']

    if not date_min:
        date_min = datetime.datetime(year=2022, month=1, day=1)
    if not date_max:
        date_max = datetime.datetime(year=2022, month=12, day=31)

    # Create a list of customer indices that will have a basket (customers can occur multiple times)
    customer_ids_rnd = random.choices(customer_ids, k=n_baskets)
    baskets = []
    for customer_id in customer_ids_rnd:
        # Create a basket for this customer
        date = faker_obj.date_between(date_min, date_max)
        basket = create_basket(
            customer_id, product_ids, lines_min=n_lines_min, lines_max=n_lines_max,
            max_amount_per_line=max_amount_per_line, purchase_date=date)
        # concatenate lines of this basket
        baskets += basket

    return spark.createDataFrame(baskets, cols)


def create_pos_jsons(
        faker_obj: Faker, root_path: str, customer_ids: List[str], product_ids: List[str],
        n_lines_min: int = 1, n_lines_max: int = 10, n_baskets: int = 500, max_amount_per_line: int = 10,
        date_min: datetime.datetime = None, date_max: datetime.datetime = None):

    if not date_min:
        date_min = datetime.datetime(year=2022, month=1, day=1)
    if not date_max:
        date_max = datetime.datetime(year=2022, month=12, day=31)

    # Create a list of customer indices that will have a basket (customers can occur multiple times)
    customer_ids_rnd = random.choices(customer_ids, k=n_baskets)

    for customer_id in customer_ids_rnd:
        # Create a basket for this customer
        date = faker_obj.date_between(date_min, date_max)
        ordernumber = str(faker_obj.unique.ean())
        basket = create_basket_json(
            ordernumber, customer_id, product_ids, lines_min=n_lines_min, lines_max=n_lines_max,
            max_amount_per_line=max_amount_per_line, purchase_date=date)

        date_path = os.path.join(root_path, f"date={date.strftime('%Y%m%d')}")
        json_path = os.path.join(date_path, f'{customer_id}-{ordernumber}.json')

        if not os.path.isdir(date_path):
            os.makedirs(date_path)

        with open(json_path, 'w') as fp:
            json.dump(basket, fp)


def create_basket_json(
        basket_id: str, customer_id: str, product_ids: List[str], lines_min: int = 1, lines_max: int = 10,
        max_amount_per_line: int = 10,
        purchase_date: datetime.datetime = None) -> Dict[str, Any]:
    n_lines = random.randint(lines_min, lines_max)

    if not purchase_date:
        purchase_date = datetime.datetime(year=2022, month=6, day=1)

    dt = purchase_date.strftime('%Y%m%d%H%M%S')

    # Each basket has a customer_id, product_id and count
    basket = {'customer_id': customer_id, 'datetime': dt, 'order_number': basket_id}
    items = []
    for _ in range(n_lines):
        product_id = random.choice(product_ids)
        amount = random.randint(1, max_amount_per_line)
        items.append({'product_id': product_id, 'amount': amount})

    basket['items'] = items

    return basket


def create_basket(customer_id: str, product_ids: List[str], lines_min: int = 1, lines_max: int = 10,
                  max_amount_per_line: int = 10,
                  purchase_date: datetime.datetime = None) -> List[Tuple[str, str, int, str]]:
    n_lines = random.randint(lines_min, lines_max)

    if not purchase_date:
        purchase_date = datetime.datetime(year=2022, month=6, day=1)

    date = purchase_date.strftime('%Y-%m-%d')

    # Each line has a customer_id, product_id and count
    lines = []
    for line in range(n_lines):
        product_id = random.choice(product_ids)
        amount = random.randint(1, max_amount_per_line)
        lines.append((customer_id, product_id, amount, date))

    return lines


def unique_value_list(sdf: DataFrame, colname: str) -> List[Any]:
    return [r[0] for r in sdf.select(colname).distinct().toLocalIterator()]


# COMMAND ----------

import shutil

fake = Faker()

Faker.seed(42)
random.seed(43)

raw_data_root_path = '/dbfs/data/raw'
path_pos = os.path.join(raw_data_root_path, 'pos')
path_cust = os.path.join(raw_data_root_path, 'customers.csv')
path_prod = os.path.join(raw_data_root_path, 'products.csv')

shutil.rmtree(path_pos, ignore_errors=True)
shutil.rmtree(path_cust, ignore_errors=True)
shutil.rmtree(path_prod, ignore_errors=True)

sdf_customer_data = create_customer_table(fake, n_customers=300)
sdf_product_data = create_products_table(fake)

customer_ids = unique_value_list(sdf_customer_data, 'customer_id')
product_ids = unique_value_list(sdf_product_data, 'product_id')

#sdf_line_items = create_pos_table(fake, customer_ids, product_ids, n_lines_min=1, n_lines_max=10, n_baskets=500, max_amount_per_line=10)
create_pos_jsons(fake, path_pos, customer_ids, product_ids, n_lines_min=1, n_lines_max=10, n_baskets=700, max_amount_per_line=10, date_min=datetime.datetime(year=2022, month=4, day=1), date_max=datetime.datetime(year=2022, month=7, day=1))

# COMMAND ----------

if not os.path.isdir(raw_data_root_path):
    os.makedirs(raw_data_root_path)
    
sdf_customer_data.toPandas().to_csv(path_cust, header=True, index=False)
sdf_product_data.toPandas().to_csv(path_prod, header=True, index=False)

# COMMAND ----------


