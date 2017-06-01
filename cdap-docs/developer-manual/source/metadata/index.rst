.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

:hide-toc: true

.. _metadata:
.. _metadata-cdap-metadata:

========
Metadata
========

.. toctree::
   :maxdepth: 1
   
    System Metadata <system-metadata>
    Discovery and Lineage <discovery-lineage>
    Audit Logging <audit-logging>
    CDAP Metadata UI <metadata-ui>


*Metadata* is an important capability of CDAP. CDAP Metadata helps show how datasets and
programs are related to each other and helps in understanding the impact of a change
before the change is made.

These features provide full visibility into the impact of changes while providing an audit
trail of access to datasets by programs and applications. Together, they give a clear view
when identifying trusted data sources and enable the ability to track the trail of
sensitive data.

CDAP captures metadata from many different sources |---| as well as those specified by a
user |---| on different entities and objects. The container model of CDAP provides for the
seamless aggregation of a wide variety of machine-generated metadata that is automatically
associated with datasets. This gives developers and data scientists flexibility when
innovating and building solutions on Hadoop, without the worry of maintaining compliance
and governance for every application.

CDAP metadata |---| consisting of **properties** (a list of key-value pairs) or **tags** (a
list of keys) |---| can be used to annotate artifacts, applications, programs, datasets,
streams, and views.

Using the CDAP :ref:`Metadata HTTP RESTful API <http-restful-api-metadata>`, you can set,
retrieve, and delete these metadata annotations.

Metadata keys, values, and tags must conform to the CDAP :ref:`supported characters 
<supported-characters>`, and are limited to 50 characters in length. The entire metadata
object associated with a single entity is limited to 10K bytes in size.


.. |system-metadata| replace:: **System Metadata:**
.. _system-metadata: system-metadata.html

- |system-metadata|_ While CDAP allows users to tag entities with metadata properties and
  tags, it also tags entities with system properties and tags (:ref:`system metadata
  <metadata-system-metadata>`) by default.


.. |discovery-lineage| replace:: **Discovery and Lineage:**
.. _discovery-lineage: discovery-lineage.html

- |discovery-lineage|_ Metadata can be used to tag different CDAP components so that they
  are easily :ref:`discovered <metadata-discovery>`, identifiable, and managed. :ref:`Lineage
  <metadata-lineage>` shows |---| for a specified time range |---| all data
  access of the entity, and details of where that access originated from.


.. |audit-logging| replace:: **Audit Logging:**
.. _audit-logging: audit-logging.html

- |audit-logging|_ provides a chronological ledger containing evidence of operations or
  changes on CDAP entities.


.. |metadata-ui| replace:: **CDAP Metadata UI:**
.. _metadata-ui: metadata-ui.html

- |metadata-ui|_ lets you see how data is flowing into and out of datasets, streams, and
  stream views.


.. _metadata-navigator-integration:

.. |navigator-integration| replace:: **Cloudera Navigator Integration:**

- |navigator-integration| CDAP Metadata can be pushed to Cloudera Navigator for metadata discovery
  and search. Refer to :ref:`Cloudera Navigator Integration <navigator-integration>` for
  more information.
