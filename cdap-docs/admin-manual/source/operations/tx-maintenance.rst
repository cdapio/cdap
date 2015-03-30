.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _tx-maintenance:

===============================
Transaction Service Maintenance
===============================

.. highlight:: console

The Transaction Service keeps track of all invalid transactions to exclude their writes from all future reads. 
Over time, this *invalid* list can grow and lead to performance degradation. To avoid that, you can prune the *invalid*
list after the data of invalid transactions has been removed |---| which happens during major HBase compactions of the
transactional tables.

After a major compaction that started at time ``t`` completes, the invalid transaction list can be safely pruned
until ``t - 1 day`` using :ref:`a call <http-restful-api-transactions-truncate>` 
from the :ref:`Transaction Service HTTP RESTful API <http-restful-api-transactions-truncate>`.

The current length of the invalid transaction list can be obtained using 
:ref:`a call <http-restful-api-transactions-number>` 
from the :ref:`Transaction Service HTTP RESTful API <http-restful-api-transactions-number>`.
