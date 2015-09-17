.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _metadata:

========
Metadata
========

Overview
========
Metadata |---| consisting of **properties** (a list of key-value pairs) or **tags** (a
list of keys) |---| can be used to annotate datasets, streams, programs, and applications.

Using the CDAP :ref:`Metadata HTTP RESTful API <http-restful-api-metadata>`, you can set,
retrieve, and delete the metadata annotations of applications, datasets, streams, and
programs in CDAP.

Metadata keys, values, and tags must conform to the CDAP :ref:`supported characters 
<supported-characters>`, and are limited to 50 characters in length. The entire metadata
object associated with a single entity is limited to 10K bytes in size.

Metadata can be used to tag different CDAP components so that they are easily identifiable
and managed. You can tag a datasets as *experimental* or an application as *production*.

Metadata can be **searched**, either to find entities:

- that have a particular **value** for *any key* in their properties;
- that have a particular **key** with a particular *value* in their properties; or
- that have a particular **tag**.


.. _metadata-lineage:

Lineage
=======
**Lineage** can be retrieved for dataset and stream entities. A lineage shows
|---| for a specified time range |---| all data access of the entity, and details of where
that access originated from.

For example: with a stream, writing to a stream may take place from a worker, which
obtained the data from a combination of a dataset and a stream. The data in those entities
comes from possibly other entities. The number of levels of the lineage that are
calculated is set when a request is made to view the lineage of a particular entity.

In the case of streams, the lineage includes whether the access was reading or writing to
the stream. In the case of datasets, in this CDAP version, lineage can only indicate that
dataset access took place, and does not provide indication if that access was for reading
or writing. Later versions of CDAP will address this limitation.
