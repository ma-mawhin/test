import pytest
import requests
from requests.exceptions import RequestException


class FakeSession:
    def __init__(
        self,
        pat=None,
        tenant_uri="a_tenant",
    ):
        self.tenant_uri = tenant_uri
        self.set_pat(pat)
        self.create_session()

    def set_pat(self, pat):
        self.pat = pat

    def create_session(self):
        s = requests.Session()
        s.headers = {"Authorization": f"Bearer {self.pat}"}
        self.session = s


class FakeStuff(FakeSession):
    def __init__(
        self,
        pat=None,
        tenant_uri="b_tenant",
    ):
        super().__init__(pat, tenant_uri)

    def fake_func_1(self) -> str:
        res = self.session.get(url="whatever")
        res.raise_for_status
        return [x.get("b") for x in res.json().get("a")]

    def fake_func_2(self, username) -> str:
        res = self.session.get("whatever")
        total_results = res.json()["total"]
        if total_results == 0:
            raise ValueError(f"No user found with username {username}")


class MockResponse:
    def __init__(self, json_path, status_code=200):
        self.status_code = status_code
        self.json_path = json_path

    def json(self):
        return self.json_path

    def raise_for_status():
        raise RequestException("Oops")


class MockSession:
    def __init__(self, MockResponse, url=None, headers=None):
        self.headers = headers
        self.response = MockResponse
        self.url = None

    def get(self, url):
        self.url = url
        return self.response

    def post(self):
        return self.response(status_code=409)


def test_fake_func_1(mocker):
    fake_response = MockResponse(
        json_path={"a": [{"b": 1}, {"b": 2}, {"c": 3}], "total": 0}, status_code=200
    )
    mocker.patch(
        "requests.Session",
        return_value=MockSession(MockResponse=fake_response, url="url"),
    )
    actual = FakeStuff("b").fake_func_1()
    expected = [1, 2, None]
    assert actual == expected


def test_fake_func_2(mocker):
    mocker.patch(
        "requests.Session",
        return_value=MockSession(MockResponse(json_path={"total":0}, status_code=400)),
    )
    with pytest.raises(ValueError) as e:
        FakeStuff("b").fake_func_2("Morgane")
    assert "No user found with username Morgane" in str(e)
