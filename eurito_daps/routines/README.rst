Production pipelines
====================

We use luigi routines to orchestrate our pipelines. The batching procedure relies on batchables as described in :code:`batchables`. Other than :code:`luigihacks.autobatch`, which is respectively documented in `Nesta's codebase <https://nesta.readthedocs.io/en/latest/nesta.core.luigihacks.html>`_, the routine procedure follows the Luigi_ documentation well.

.. _Luigi: https://luigi.readthedocs.io/en/stable/


.. automodule:: eurito_daps.routines.es_data.es_data
    :members:
    :undoc-members:
    :show-inheritance:


.. automodule:: eurito_daps.routines.centrality.centrality
    :members:
    :undoc-members:
    :show-inheritance:


.. automodule:: eurito_daps.routines.cordis.cordis_neo4j_task
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: eurito_daps.routines.openaire.openaire_to_neo4j_search
    :members:
    :undoc-members:
    :show-inheritance:
