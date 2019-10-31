from es2es import es2es
import luigi
from datetime import datetime as dt
import logging

from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.luigihacks.estask import LazyElasticsearchTask
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.orms.orm_utils import get_config, assert_correct_config

ORIGIN = ('https://search-eurito-dev-'
          'vq22tw6otqjpdh47u75bh2g7ba.'
          'eu-west-2.es.amazonaws.com')

class Es2EsTask(luigi.Task):
    date = luigi.DateParameter()
    origin_endpoint = luigi.Parameter()
    origin_index = luigi.Parameter()
    dest_endpoint = luigi.Parameter()
    dest_index = luigi.Parameter()
    test = luigi.BoolParameter()
    chunksize = luigi.IntParameter()
    do_transfer_index = luigi.BoolParameter()
    db_config_path = luigi.Parameter('mysqldb.config')

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path,
                                         "mysqldb")
        db_config["database"] = ("production" if not self.test
                                 else "dev")
        db_config["table"] = "es2es <dummy>"  # NB, not a real tbl
        update_id = "Es2Es_{}_{}_{}".format(self.date,
                                            self.origin_index,
                                            self.test)
        return MySqlTarget(update_id=update_id, **db_config)


    def run(self):
        logging.warning(f'\nTransferring data to "{self.dest_endpoint}"'
                        f'at index "{self.dest_index}" from '
                        f'"{self.origin_endpoint}" at '
                        f'"{self.origin_index}"\n')
        es2es(origin_endpoint=self.origin_endpoint,
              origin_index=self.origin_index,
              dest_endpoint=self.dest_endpoint,
              dest_index=self.dest_index,
              limit=1000 if self.test else None,
              do_transfer_index=self.do_transfer_index,
              chunksize=self.chunksize)
        self.output().touch()


class EsLolveltyTask(LazyElasticsearchTask):
    date = luigi.DateParameter()
    origin_endpoint = luigi.Parameter(default=ORIGIN)
    origin_index = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    process_batch_size = luigi.IntParameter(default=5000)
    do_transfer_index = luigi.BoolParameter(default=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        # Get and check the config
        key = f"{self.dataset}_{'dev' if self.test else 'prod'}"
        es_config = get_config('elasticsearch.config', key)
        assert_correct_config(self.test, es_config, key)
        return Es2EsTask(date=self.date,
                         origin_endpoint=self.origin_endpoint,
                         origin_index=self.origin_index,
                         dest_endpoint=es_config['host'],
                         dest_index=es_config['index'],
                         test=self.test,
                         chunksize=self.process_batch_size,
                         do_transfer_index=self.do_transfer_index)


class RootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())

    def requires(self):
        logging.getLogger().setLevel(logging.INFO)
        keys = {'companies': {'index': 'companies_v0',
                              'score_field': 'metric_novelty_organisation',
                              'fields': ['textBody_descriptive_organisation', 
                                         'textBody_summary_organisation']},
                'patstat': {'index': 'patstat_v0',
                            'score_field': 'metric_novelty_patent',
                            'fields': ['textBody_abstract_patent']},
                'arxiv': {'index': 'arxiv_v0',
                          'score_field': 'metric_novelty_article',
                          'fields': ['textBody_abstract_article']}}

        for dataset, kwargs in keys.items():
            routine_id = f'Lol_{dataset}_{self.production}_{self.date}'
            yield EsLolveltyTask(date=self.date,
                                 routine_id=routine_id,
                                 origin_index=kwargs.pop('index'),
                                 test=not self.production,
                                 dataset=dataset,
                                 entity_type=None,
                                 kwargs=kwargs,
                                 intermediate_bucket='eurito-intermediate-batch',
                                 batchable=f3p("batchables/novelty"
                                               "/lolvelty"),
                                 env_files=[f3p("eurito_daps/"),
                                            f3p("config/mysqldb.config"),
                                            f3p("config/"
                                                "elasticsearch.config")],
                                 job_def="py36_amzn1_image",
                                 job_name=routine_id,
                                 job_queue="HighPriority",
                                 region_name="eu-west-1",
                                 poll_time=10,
                                 memory=1024,
                                 max_live_jobs=30)

