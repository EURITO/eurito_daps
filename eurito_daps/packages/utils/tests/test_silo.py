from unittest import mock

from eurito_daps.packages.utils.silo import get
from eurito_daps.packages.utils.silo import put
from eurito_daps.packages.utils.silo import s3_json_pointer


@mock.patch("eurito_daps.packages.utils.silo.boto3")
def test_s3_json_pointer(mocked_boto3):
    obj = s3_json_pointer("dataset_name", "bucket_name")
    assert 'boto3.resource().Object()' in str(obj)


@mock.patch("eurito_daps.packages.utils.silo.s3_json_pointer")
@mock.patch("eurito_daps.packages.utils.silo.json")
def test_get(mocked_json, mocked_s3_json_pointer):
    js = get("dataset_name")
    assert 'json.loads()' in str(js)


@mock.patch("eurito_daps.packages.utils.silo.s3_json_pointer")
def test_put(mocked_s3_json_pointer):
    response = put({"data": "value"}, "dataset_name")
    assert '.put()' in str(response)

