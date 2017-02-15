.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.

=====================
Discovery and Lineage
=====================


.. _metadata-discovery:

Discovery
=========
Metadata can be used to tag different CDAP components so that they are easily identifiable
and managed. This helps in discovering CDAP components.

For example, you can tag a dataset as *experimental* or an application as *production*. These
entities can then be discovered by using **search queries** with the annotated metadata. 

Using search, you can discover entities:

- that have a particular **value** for *any key* in their properties;
- that have a particular **key** with a particular *value* in their properties; or
- that have a particular **tag**.

You can find a dataset or a stream that has a "field with the given name" or a "field
with the given name and the given type".

To search metadata, you can use the :ref:`Metadata HTTP RESTful API <http-restful-api-metadata-searching>`.

.. _metadata-lineage:

Lineage
=======
**Lineage** can be retrieved for dataset and stream entities. A lineage shows
|---| for a specified time range |---| all data access of the entity, and details of where
that access originated from.

For example: with a stream, writing to a stream can take place from a worker, which may
have obtained the data from a combination of a dataset and a (different) stream. The data
in those entities can come from (possibly) other entities. The number of levels of the
lineage that are calculated is set when a request is made to view the lineage of a
particular entity.

In the case of streams, the lineage includes whether the access was reading or writing to
the stream. 

In the case of datasets, lineage can indicate if a dataset access was for reading,
writing, or both, if the methods in the dataset have appropriate :ref:`annotations
<custom-datasets-access-annotations>`. If annotations are absent, lineage can only
indicate that a dataset access took place, and does not provide indication if that access
was for reading or writing.
