from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.sql2estask import Sql2EsTask
from nesta.core.orms.arxiv_orm import Article
from nesta.core.orms.crunchbase_orm import Organization
#from eurito_daps.orms.patstat_2019_05_13 import Tls201Appln as Patent

S3_BUCKET='eurito-production-intermediate'

def kwarg_maker(dataset):
    env_files=[f3p('config/mysqldb.config'),
               f3p('config/elasticsearch.config'),
               f3p(f'schema_transformations/{dataset}.json')]
    return dict(dataset=dataset,
                env_files=env_files,
                batchable=f3p(f'batchables/elasticsearch/{dataset}'))


class RootTask(luigi.WrapperTask):
    process_batch_size = luigi.IntParameter(default=10000)
    production = luigi.BoolParameter(default=False)

    def requires(self):
        set_log_level()
        batch = (self.process_batch_size if self.production
                 else 1000)
        test = not self.production
        routine_id = 'ElasticsearchTask-{self.date}-{test}'
        default_kwargs = dict(routine_id=routine_id,
                              date=self.date,
                              db_section='nesta',
                              process_batch_size=batch,
                              drop_and_recreate=self.drop_and_recreate,
                              job_def='py36_amzn1_image',
                              job_name=routine_id,
                              job_queue='HighPriority',
                              region_name='eu-west-2',
                              poll_time=10,
                              max_live_jobs=100,
                              db_config_env='MYSQLDB',
                              test=test,
                              intermediate_bucket=S3_BUCKET)
        
        params = (('arxiv', 'article', Article.id),)
        #('crunchbase', 'company', Organization.id),
        #('patstat', 'patent', Patent.appln_id))
        
        for dataset, entity_type, id_field in params:
            yield Sql2EsTask(id_field=id_field,
                             entity_type=entity_type,
                             **kwarg_maker(dataset),
                             **default_kwargs)

