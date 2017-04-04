.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=================
Data Prep Service
=================

Data Prep is integrated as a CDAP Service to support HTTP RESTful-based
interactive wrangling of data. The main objective of this service is to
make it simple and easy to interactively apply the directives required
for parsing a given data set. The service is not intended to replace
full-scale big data processing; it is primarily used to interactively
apply directives on a sample of your data.

The base endpoint is:

::

    http://<hostname>:11015/v3/namespaces/<namespace>/apps/dataprep/services/service/methods

These services are provided:

-  `Administration and Management <docs/service/admin.md>`__
-  `Directive Execution <docs/service/execution.md>`__
-  `Column Type Detection and Statistics <docs/service/statistics.md>`__
-  `Column Name Validation <docs/service/validation.md>`__

The `Request Format Specification <docs/service/request.md>`__ describes
the format that is used for sending a request to the back-end.
