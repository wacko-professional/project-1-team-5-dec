import requests

class ExchangeRatesClient:

    def __init__(self, api_base_url: str, api_access_key: str):
        self.api_base_url = api_base_url
        self.api_access_key = api_access_key

    def get_rates(self, base_currency: str, date: str):
        url = f'{self.api_base_url}{date}'
        params = {
            "access_key": self.api_access_key,
            "base": base_currency
        }

        response = requests.get(url=url, params=params)

        if response is not None:
            response_json = response.json()
            if response_json.get("rates") is not None:
                return response_json.get("rates")
            else:
                raise Exception(f'Failed to extract data from Exchange Rates API. Status Code: {response.status_code}. Response: {response.text}')
        else:
            raise Exception(f'Failed to get a response from the Exchange Rates API.')
    
    def get_rates_date_range(self, base_currency: str, start_date: str, end_date: str):
        url=f'{self.api_base_url}timeseries'
        params = {
            "access_key": self.api_base_url,
            "start_date": start_date,
            "end_date": end_date
        }

        response = requests.get(url=url, params=params)

        if response is not None:
            response_json = response.json()
            if response_json.get("rates") is not None:
                return response_json.get("rates")
            else:
                raise Exception(f'Failed to extract data from Exchange Rates API. Status Code: {response.status_code}. Response: {response.text}')
        else:
            raise Exception(f'Failed to get a response from the Exchange Rates API.')