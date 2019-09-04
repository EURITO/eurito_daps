from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from ast import literal_eval
import boto3
import json
import logging
import os

from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import load_json_from_pathstub
from nesta.core.orms.orm_utils import object_to_dict
from nesta.core.orms.arxiv_orm import Article

def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']

    db_name = os.environ["BATCHPAR_db_name"]
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = int(os.environ['BATCHPAR_out_port'])
    es_index = os.environ['BATCHPAR_out_index']
    es_type = os.environ['BATCHPAR_out_type']
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]

    # database setup
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)

    # es setup
    strans_kwargs={'filename':'arxiv.json',
                   'from_key':'tier_0',
                   'to_key':'tier_1',
                   'ignore':['id']}

    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           listify_terms=True,
                           do_sort=False)

    # Collect file
    obj = boto3.resource('s3').Object(bucket, batch_file)
    ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"Got {len(ids)} ids from s3")

    nrows = 20 if test else None
    with db_session(engine) as session:
        results = (session.query(Article)
                   .filter(Article.id.in_(ids))
                   .limit(nrows))
        for obj in results:
            article = object_to_dict(obj)
            uid = article.pop('id')
            _row = es.index(index=es_index, doc_type=es_type,
                            id=uid, body=article)

    
if __name__ == "__main__":
    run()
