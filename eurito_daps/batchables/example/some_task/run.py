'''
run.py (batch_example)
======================

The batchable for the :code:`routines.examples.batch_example`,
which simply increments a muppet's age by one unit.
'''

import os
import logging
from nesta.production.orms.orm_utils import load_json_from_pathstub
from nesta.production.orms.orm_utils import setup_es
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from nesta.production.luigihacks.elasticsearchplus import ElasticsearchPlus
from eurito_daps.packages.utils import silo


def run():
    '''Gets the name and age of the muppet, and increments the age.
    The result is transferred to S3.'''

    # Get parameters for the batch job
    dataset = os.environ["BATCHPAR_dataset"]
    start_index = int(os.environ["BATCHPAR_start_index"])
    end_index = int(os.environ["BATCHPAR_end_index"])
    age_increment = int(os.environ["BATCHPAR_age_increment"])
    
    es_host = os.environ["BATCHPAR_outinfo"]
    es_port = os.environ["BATCHPAR_out_port"]
    es_index = os.environ["BATCHPAR_out_index"]
    es_type = os.environ["BATCHPAR_out_type"]
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]

    # Get the input data and modify it based on the input parameters
    data = silo.get("example")[start_index:end_index]
    for row in data:
        row["age"] = row["age"] + age_increment
    
    # Connect to ES    
    field_null_mapping = load_json_from_pathstub("tier_1/field_null_mappings/",
                                                 "example.json")    
    strans_fpath = find_filepath_from_pathstub("tier_1/schema_transformations/"
                                               "example.json")    
    strans_kwargs={'filename':strans_fpath,
                   'from_key':'tier_0',
                   'to_key':'tier_1',
                   'ignore':['id']}

    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           field_null_mapping=field_null_mapping,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           country_detection=False,
                           caps_to_camel_case=True)

    for uid, row in enumerate(data):
        uid = uid + start_index
        es.index(index=es_index, 
                 doc_type=es_type, id=uid, body=row)

    # Also upload the data to S3
    silo.put(data, dataset)


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    if 'BATCHPAR_outinfo' not in os.environ:
        es, es_config = setup_es(es_mode="dev", test_mode=True, 
                                 reindex_mode=True, 
                                 dataset='example', 
                                 aliases='example')

        #environ = {"AWSBATCHTEST": "",  ## << This means don't write to ES
        environ = {"BATCHPAR_aws_auth_region": es_config["region"],
                   "BATCHPAR_outinfo": es_config["host"],
                   "BATCHPAR_dataset" : "example",
                   "BATCHPAR_done":"False",
                   "BATCHPAR_age_increment": "-3",
                   "BATCHPAR_start_index":"0",
                   "BATCHPAR_end_index":"3",
                   "BATCHPAR_out_type": es_config["type"],
                   "BATCHPAR_out_port": es_config["port"],
                   "BATCHPAR_out_index":es_config["index"],
                   "BATCHPAR_entity_type":"muppet"}
        for k, v in environ.items():
            os.environ[k] = v
    run()
