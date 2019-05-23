import boto3
import json


def s3_json_pointer(dataset, bucket, extension=".json"):
    """Retrieve the S3 object pointer, assumed to be json.

    Args:
        dataset (str): Name of the dataset (the S3 object key will
                       be constructed as :obj:`dataset` + :obj:`extension`.
        bucket (str): Name of S3 bucket to find the object.
        extension (str): Extension of the S3 object (the S3 object key will
                         be built from :obj:`dataset` + :obj:`extension`self.
    Returns:
        s3.Object instance.
    """
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, f"{dataset}{extension}")
    return obj


def get(dataset, bucket="eurito-inputs"):
    """Retrieve a json object from S3.

    Args:
        dataset (str): Name of the dataset (the S3 object key will
                       be constructed as :obj:`dataset` + :obj:`extension`.
        bucket (str): Name of S3 bucket to find the object.

    Returns:
        Retrieved data
    """
    obj = s3_json_pointer(dataset, bucket)
    _body = obj.get()['Body'].read()
    return json.loads(_body.decode())


def put(json_data, dataset, bucket="eurito-outputs"):
    """Persistify a json object to S3.

    Args:
        dataset (str): Name of the dataset (the S3 object key will
                       be constructed as :obj:`dataset` + :obj:`extension`.
        bucket (str): Name of S3 bucket to find the object.

    Returns:
        PUT request response
    """
    obj = s3_json_pointer(dataset, bucket)
    _body = bytes(json.dumps(json_data).encode('UTF-8'))
    return obj.put(Body=_body)
