.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Applications
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. _cdap-apps-index:

=================
CDAP Applications
=================


CDAP comes packaged with these additional extensions:

.. |hydrator| replace:: **Cask Hydrator and ETL (Extract, Transform, and Load) Pipelines:**
.. _hydrator: hydrator/index.html

- |hydrator|_ With CDAP are several system artifacts to create different types of
  applications simply by configuring the system artifacts and not by writing any code at all.

  Cask Hydrator lets you create ETL (Extract, Transform, and Load) pipelines by
  configuring these system artifacts (or your own custom artifacts) using the *Cask
  Hydrator Studio*. An application created from a configured system artifact following the
  ETL pattern is referred to as an *ETL pipeline* or (interchangeably) as an *ETL
  application*.

  The framework is extensible: users can write their own artifacts if they so chose, and
  can manage the lifecycle of their custom applications using CDAP. In the future, a
  variety of system artifacts will be delivered with different capabilities.

.. |tracker| replace:: **Cask Tracker:**
.. _tracker: tracker/index.html

- |tracker|_ A self-service CDAP extension that automatically captures metadata and
  lets you see how data is flowing into and out of datasets, streams, and stream views.

.. |dqa| replace:: **Data Quality Application:**
.. _dqa: data-quality/index.html

- |dqa|_ An extensible CDAP application to help determine the quality of your data using
  its out-of-the-box functionality and libraries. Similar to an *ETL pipeline*, an
  application built following the Data Quality pattern is referred to as a *Data Quality
  application*. It can be configured from a system artifact and does not require writing
  any code.