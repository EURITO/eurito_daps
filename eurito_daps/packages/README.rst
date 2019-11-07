Data sources and core processing
================================

In EURITO we predominantly use four data sources:

* EU-funded research from *CORDIS*
* Technical EU research on *arXiv*
* EU Patents from *PATSTAT*
* EU Companies [under license, we can't specify the source publicly. Contact us for more details!]


CORDIS
------

Data from the CORDIS's `H2020 API <http://cordis.europa.eu/data/cordis-h2020projects.csv>`_ and `FP7 API <http://cordis.europa.eu/data/cordis-fp7projects.csv>`_ funded projects is extracted using code found `in this repository <https://nesta.readthedocs.io/en/dev/nesta.core.routines.cordis.html>`_.

In total, 51250 organisations and 50640 projects were extracted from the API. There are 1102 proposal calls, 245465 publications and 34507 reports. In total 6545 are associated with the projects.

Software outputs are associated with the projects, using `OpenAIRE API <http://api.openaire.eu/oai_pmh>`_.

All of these entities are then linked together, and stored using a `neo4j <https://neo4j.com/>`_ graph database. The code for automatically piping the data in neo4j is provided `here <https://eurito.readthedocs.io/en/dev/_modules/packages/utils/cordis_neo4j.py>`_.

.. automodule:: eurito_daps.packages.cordis.cordis_neo4j
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: eurito_daps.packages.utils.openaire_utils
    :members:
    :undoc-members:
    :show-inheritance:

arXiv
------

All articles from `arXiv <https://arxiv.org>`_, which is the world's premier repository of pre-prints of articles in the physical, quantitative and computational sciences, are already automatically collected, geocoded (using `GRID <https://www.grid.ac/>`_) and enriched with topics (using `MAG <https://docs.microsoft.com/en-us/azure/cognitive-services/academic-knowledge/home>`_). Articles are assigned to EU NUTS regions (at all levels) using nesta's `nuts-finder <https://pypi.org/project/nuts-finder/>`_ python package.

Data is transferred to EURITO's `elasticsearch <https://www.elastic.co/guide/index.html>`_ server via nesta's `es2es <https://pypi.org/project/es2es/>`_ package. The `lolvelty algorithm <https://github.com/nestauk/nesta/blob/dev/nesta/packages/novelty/lolvelty.py>`_ is then applied to the data in order to generate a novelty metric for each article. This procedure is bettter described in `this blog <https://towardsdatascience.com/big-fast-nlp-with-elasticsearch-72ffd7ef8f2e>`_ (see "Defining novelty").

The indicators using this data source are presented in `this other EURITO repository <https://github.com/EURITO/query_indicators>`_.

In total, 1598033 articles have been processed, of which 459371 have authors based in EU nations.

PATSTAT
-------

All patents from the PATSTAT service have been collected in nesta's own database using nesta's `pypatstat <https://github.com/nestauk/pypatstat>`_ library. Since this database is very large, we have selected patents which belong to a patent family with a granted patent first published after the year 2000, with at least one person or organisation (inventor or applicant) based in an EU member state. This leads to 1552303 patents in the database.

Data is transferred to EURITO's `elasticsearch <https://www.elastic.co/guide/index.html>`_ server via nesta's `es2es <https://pypi.org/project/es2es/>`_ package. The `lolvelty algorithm <https://github.com/nestauk/nesta/blob/dev/nesta/packages/novelty/lolvelty.py>`_ is then applied to the data in order to generate a novelty metric for each article. This procedure is bettter described in `this blog <https://towardsdatascience.com/big-fast-nlp-with-elasticsearch-72ffd7ef8f2e>`_ (see "Defining novelty").

The indicators using this data source are presented in `this other EURITO repository <https://github.com/EURITO/query_indicators>`_.


Companies
---------

We have acquired private-sector company data under license. The dataset contains 550540 companies, of which 133641 are based in the EU.

Data is transferred to EURITO's `elasticsearch <https://www.elastic.co/guide/index.html>`_ server via nesta's `es2es <https://pypi.org/project/es2es/>`_ package. The `lolvelty algorithm <https://github.com/nestauk/nesta/blob/dev/nesta/packages/novelty/lolvelty.py>`_ is then applied to the data in order to generate a novelty metric for each article. This procedure is bettter described in `this blog <https://towardsdatascience.com/big-fast-nlp-with-elasticsearch-72ffd7ef8f2e>`_ (see "Defining novelty").

The indicators using this data source are presented in `this other EURITO repository <https://github.com/EURITO/query_indicators>`_.
