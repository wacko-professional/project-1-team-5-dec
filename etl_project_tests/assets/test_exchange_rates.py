from etl_project.assets.exchange_rates import extract_exchange_rates, transform_exchange_rates
from etl_project.connectors.postgresql import PostgresqlClient
from etl_project.connectors.exchange_rates import ExchangeRatesClient
import yaml
from pathlib import Path
import os
import pytest
from dotenv import load_dotenv
from datetime import datetime, date
import pandas as pd

def pytest_namespace():
    return {'df_forex': None}

@pytest.fixture
def setup_extract():
    load_dotenv()

@pytest.fixture
def forex_test_data_for_extract() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(os.getcwd(), "etl_project_tests/resources/forex.csv"))

    return df.drop(columns=['base_currency', 'date'])

@pytest.fixture
def forex_test_data_for_transform() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(os.getcwd(), "etl_project_tests/resources/forex.csv"))
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").dt.date

    return df


def test_extract_exchange_rates(setup_extract, forex_test_data_for_extract):
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists:
        with open(yaml_file_path) as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file!")
    
    base_url = yaml_config.get("base_url")
    base_currency = yaml_config.get("base_currency")
    
    ACCESS_KEY = os.environ.get("ACCESS_KEY")

    date_requested = datetime(year=2023, month=9, day=1).date()
    
    exchange_rate_client = ExchangeRatesClient(api_base_url=base_url, api_access_key=ACCESS_KEY)

    df_forex = extract_exchange_rates(exchange_rate_client=exchange_rate_client, base_currency=base_currency, date_requested=date_requested)

    pytest.df_forex = df_forex

    pd.testing.assert_frame_equal(left=forex_test_data_for_extract, right=df_forex)

    # forex_csv_path = Path('resources/forex.csv')
    # forex_csv_path.parent.mkdir(parents=True, exist_ok=True)
    # df_forex_transformed.to_csv("resources/forex.csv")

def test_transform_exchange_rates(forex_test_data_for_extract, forex_test_data_for_transform):
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists:
        with open(yaml_file_path) as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file!")
    
    base_url = yaml_config.get("base_url")
    base_currency = yaml_config.get("base_currency")

    date_requested = datetime(year=2023, month=9, day=1).date()

    # print(pytest.df_forex)
    df_forex = pytest.df_forex

    df_forex_transformed = transform_exchange_rates(df=df_forex, base_currency=base_currency, date=date_requested)

    pd.testing.assert_frame_equal(left=forex_test_data_for_transform, right=df_forex_transformed)