.. meta::
    :author: Cask Data, Inc.
    :description: Guides to the Cask Data Application Platform
    :copyright: Copyright © 2017 Cask Data, Inc.

:hide-relations: true
:hide-right-sidebar: true

======
Guides
======

.. role:: link-black

.. default-role:: link-black

.. |user-guide| replace:: **User Guide**
.. _user-guide: user-guide/index.html

.. |user-guide-prep-black| replace:: `Data Preparation:`
.. _user-guide-prep-black: user-guide/data-preparation/index.html

.. |user-guide-pipe-black| replace:: `Data Pipelines:`
.. _user-guide-pipe-black: user-guide/pipelines/index.html

.. |user-guide-mmds-black| replace:: `Analytics:`
.. _user-guide-mmds-black: user-guide/mmds/index.html

- |user-guide|_

  - |user-guide-prep-black|_ Documentation on usage of Data Preparation and Data Preparation transforms

  - |user-guide-pipe-black|_ Documentation on usage of Data Pipelines UI

  - |user-guide-mmds-black|_ Interactive, UI-driven analytics and machine learning


.. |developer-manual| replace:: **Developer Manual**
.. _developer-manual: developer-manual/index.html

.. |dev-man-gsd-black| replace:: `Getting Started Developing:`
.. _dev-man-gsd-black: developer-manual/getting-started/index.html

.. |dev-man-o-black| replace:: `Overview:`
.. _dev-man-o-black: developer-manual/overview/index.html

.. |dev-man-bb-black| replace:: `Building Blocks:`
.. _dev-man-bb-black: developer-manual/building-blocks/index.html

.. |dev-man-m-black| replace:: `Metadata:`
.. _dev-man-m-black: developer-manual/metadata/index.html

.. |dev-man-p-black| replace:: `Pipelines:`
.. _dev-man-p-black: developer-manual/pipelines/index.html

.. |dev-man-cr-black| replace:: `Cloud Runtimes:`
.. _dev-man-cr-black: developer-manual/cloud-runtimes/index.html

.. |dev-man-s-black| replace:: `Security:`
.. _dev-man-s-black: developer-manual/security/index.html

.. |dev-man-tad-black| replace:: `Testing and Debugging:`
.. _dev-man-tad-black: developer-manual/testing/index.html

.. |dev-man-id-black| replace:: `Ingesting Data:`
.. _dev-man-id-black: developer-manual/ingesting-tools/index.html

.. |dev-man-at-black| replace:: `Advanced Topics:`
.. _dev-man-at-black: developer-manual/advanced/index.html

- |developer-manual|_

  - |dev-man-gsd-black|_ A quick, hands-on introduction to developing with CDAP
  - |dev-man-o-black|_ The overall architecture, abstractions, modes, and components behind CDAP
  - |dev-man-bb-black|_ The two core abstractions in CDAP: *Data* and *Applications*, and their components
  - |dev-man-m-black|_ CDAP can automatically capture metadata and let you see **how data is flowing**
  - |dev-man-p-black|_ A capability of CDAP that that allows users to **build, deploy, and manage data pipelines**
  - |dev-man-cr-black|_ Set up profiles to run pipelines in different cloud environments
  - |dev-man-s-black|_ Perimeter security, configuration and client authentication
  - |dev-man-tad-black|_ Test framework plus tools and practices for debugging your applications
  - |dev-man-id-black|_ Different techniques for ingesting data into CDAP
  - |dev-man-at-black|_ Adding a custom logback, best practices for CDAP development,
    class loading in CDAP, configuring program resources and retry policies


.. |admin-manual| replace:: **Administration Manual**
.. _admin-manual: admin-manual/index.html

.. |admin-man-i-black| replace:: `Installation:`
.. _admin-man-i-black: admin-manual/installation/index.html

.. |admin-man-s-black| replace:: `Security:`
.. _admin-man-s-black: admin-manual/security/index.html

.. |admin-man-o-black| replace:: `Operations:`
.. _admin-man-o-black: admin-manual/operations/index.html

.. |admin-man-a-black| replace:: `Appendices:`
.. _admin-man-a-black: admin-manual/appendices/index.html

- |admin-manual|_

  - |admin-man-i-black|_ Putting CDAP into production, with installation, configuration and upgrading for
    different distributions
  - |admin-man-s-black|_ CDAP supports securing clusters using a perimeter security model
  - |admin-man-o-black|_ Logging, monitoring, metrics, runtime arguments, scaling instances, resource
    guarantees, transaction service maintenance, and introduces the CDAP UI
  - |admin-man-a-black|_ Covers the CDAP installation and security configuration files


.. |integrations| replace:: **Integrations**
.. _integrations: integrations/index.html

.. |integ-man-hub-black| replace:: `Hub:`
.. _integ-man-hub-black: integrations/hub.html

.. |integ-man-cl-black| replace:: `Cloudera:`
.. _integ-man-cl-black: integrations/partners/cloudera/index.html

.. |integ-man-as-black| replace:: `Apache Sentry:`
.. _integ-man-as-black: integrations/apache-sentry.html

.. |integ-man-ar-black| replace:: `Apache Ranger:`
.. _integ-man-ar-black: integrations/apache-ranger.html

.. |integ-man-ah-black| replace:: `Apache Hadoop KMS:`
.. _integ-man-ah-black: integrations/hadoop-kms.html

.. |integ-man-jd-black| replace:: `JDBC:`
.. _integ-man-jd-black: integrations/jdbc.html

.. |integ-man-od-black| replace:: `ODBC:`
.. _integ-man-od-black: integrations/odbc.html

.. |integ-man-pe-black| replace:: `Pentaho:`
.. _integ-man-pe-black: integrations/pentaho.html

.. |integ-man-sq-black| replace:: `Squirrel:`
.. _integ-man-sq-black: integrations/squirrel.html


- |integrations|_

  - |integ-man-hub-black|_ A source for re-usable applications, data, and code for all CDAP users
  - |integ-man-cl-black|_ Integrating CDAP into Cloudera, using Cloudera Manager and running interactive queries with Impala
  - |integ-man-as-black|_ Configuring and integrating CDAP with *Apache Sentry*
  - |integ-man-ar-black|_ Configuring and integrating CDAP with *Apache Ranger*
  - |integ-man-ah-black|_ Configuring and integrating CDAP with *Apache Hadoop Key Management Service (KMS)*
  - |integ-man-jd-black|_ The CDAP JDBC driver, included with CDAP
  - |integ-man-od-black|_ The CDAP ODBC driver available for CDAP
  - |integ-man-pe-black|_ *Pentaho Data Integration*, a business intelligence tool that can be used with CDAP
  - |integ-man-sq-black|_ *SquirrelSQL*, a simple JDBC client that can be integrated with CDAP


.. |examples-manual| replace:: **Guides**
.. _examples-manual: examples-manual/index.html

.. |ex-man-htg-black| replace:: `How-To Guides:`
.. _ex-man-htg-black: examples-manual/how-to-guides/index.html

- |examples-manual|_

  - |ex-man-htg-black|_ Designed to be completed in 15-30 minutes, these guides provide quick, hands-on instructions
