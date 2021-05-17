"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import requests

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from base_python import AbstractSource, HttpStream, Stream
from base_python.cdk.streams.auth.token import TokenAuthenticator


class DbtCloudStream(HttpStream, ABC):
    url_base = 'https://cloud.getdbt.com/api/v2/'

    def __init__(self, account_id: str, **kwargs):
        super().__init__(**kwargs)
        self.account_id = account_id

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None,
                       next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        # DBT uses offset and limit to paginate.
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("data", [])


class Accounts(DbtCloudStream):
    name = "accounts"
    primary_key = "id"

    def path(self, **kwargs):
        return 'accounts'


class Projects(DbtCloudStream):
    name = "projects"
    primary_key = ""

    def path(self, **kwargs):
        return f"/accounts/{self.account_id}/projects"


class Jobs(DbtCloudStream):
    name = "jobs"
    primary_key = ""

    def path(self, **kwargs) -> str:
        return f"/accounts/{self.account_id}/jobs"


class SourceDbtCloud(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        token = config['authorization']
        account_id = config['account_id']

        get_accounts = "https://cloud.getdbt.com/api/v2/accounts/{accountId}".format(accountId=account_id)

        try:
            requests.get(
                url=get_accounts,
                headers={'Authorization': "Bearer " + token},
            )
            return True, None
        except requests.HTTPError as error:
            return False, error

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(config['authorization'])
        args = {"authenticator": auth, "account_id": config["account_id"]}

        return [
            Accounts(**args),
            Projects(**args),
            Jobs(**args)
        ]
