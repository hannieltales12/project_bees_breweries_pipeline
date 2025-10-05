import requests
import os
import json
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from bees_breweries_pipeline.tools.const.breweries_package import (
    BreweriesPackage,
)


class LandingBreweriesTask(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(
        self,
        context,
    ) -> None:
        """
        Method responsible for extracting the list of breweries from the endpoint:
        https://api.openbrewerydb.org/v1/breweries.
        And saving it in the landing layer in JSON format

        Raises:
            Exception:
                1. If the request is not successful;
                2. If the data is empty;
                3. If there is an error saving the data;
        """

        breweries_list = self._extract_breweries(
            BreweriesPackage.BASE_URL, BreweriesPackage.BREWERIES_HEADERS
        )

        dag_id = context['dag'].dag_id

        self._save_data(breweries_list, dag_id)

    def _extract_breweries(
        self, base_url: str, breweries_headers: dict
    ) -> list:
        """
        Make a request to the API list of breweries

        Args:
            base_url (str): Base URL of the API
            breweries_headers (dict): Headers for the API request

        Returns:
            list: List of breweries
        """

        print("Making a request to the API list of breweries")

        # Control variable for pagination
        per_page = 100

        # Control variable for page number
        page = 1

        breweries = []

        while True:

            url_pagination = base_url.format(per_page)

            params = {"per_page": per_page, "page": page}

            base_response = requests.get(
                url_pagination, headers=breweries_headers, params=params
            )

            if base_response.status_code != 200:
                raise Exception(
                    f"Error: {base_response.status_code} - {base_response.text}"
                )
            else:

                print(
                    f"Request successful: {base_response.status_code} - Page {page}"
                )

                data = base_response.json()

                if not data:
                    break

                breweries.extend(data)
                print(f"Page {page}, breweries found: {len(breweries)}")

                page += 1
                time.sleep(0.3)

            return breweries

    def _save_data(self, data: list, dag_id: str) -> None:
        """
        Save the data in a JSON file

        Args:
            data (list): List of breweries
            dag_id (str): Dag id for pipeline
        """

        try:

            diretory = f"/opt/airflow/.storage/landing/{dag_id}"

            if not os.path.exists(diretory):
                os.makedirs(diretory)

            json_name = os.path.join(diretory, "breweries.json")

            with open(json_name, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"Data saved in: {json_name}")

        except Exception as e:
            raise Exception(f"Error saving data: {e}")
