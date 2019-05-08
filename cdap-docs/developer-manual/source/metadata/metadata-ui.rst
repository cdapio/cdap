.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Metadata Management
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _metadata-ui:

================
CDAP Metadata UI
================

Introduction
============

The CDAP Metadata UI ("metadata management") lets you see how data is flowing into and out
of datasets, streams, and stream views.

It allows you to perform impact and root-cause analysis while delivering an audit-trail for
auditability and compliance. Metadata management furnishes access to
structured information that describes, explains, and locates data, making it easier to
retrieve, use, and manage datasets.

Metadata management also allows users to update metadata for datasets and streams. Users can add,
remove, and update tags and business properties directly in the UI. It allows users to set
a preferred dictionary of tags so that teams can use the same lexicon when updating metadata.

Metadata management's UI shows a graphical visualization of the :ref:`lineage
<metadata-lineage>` of an entity. A lineage shows |---| for a specified time range
|---| all data access of the entity, and details of where that access originated from.

**Harvest, Index, Track, and Analyze Datasets**

- Immediate, timely, and seamless capture of technical, business, and operational metadata,
  enabling faster and better traceability of all datasets.

- Through its use of lineage, it lets you understand the impact of changing a dataset on
  other datasets, processes or queries.

- Tracks the flow of data across enterprise systems and data lakes.

- Provides viewing and updating complete metadata on datasets, enabling traceability to resolve
  data issues and to improve data quality.

**Supports Standardization, Governance, and Compliance Needs**

- Provide IT with the traceability needed in governing datasets and in applying compliance
  rules through seamless integration with other extensions.

- Metadata management has consistent definitions of metadata-containing information about the data to
  reconcile differences in terminologies.

- It helps you in the understanding of the lineage of your business-critical data.

**Blends Metadata Analytics and Integrations**

- See how your datasets are being created, accessed, and processed.

Search
======
Searching in metadata management is provided by an interface similar to that of a popular search engine:

.. figure:: /_images/metadata/tracker-home-search.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

In the text box, you enter your search terms:

- Multiple search terms can be searched by separating them with a space character.
- Search terms are case-insensitive.
- Search the metadata of entities by using either a complete or partial name followed by
  an asterisk ``*``, as described in the :ref:`Metadata HTTP RESTful API
  <http-restful-api-metadata-query-terms>`.
- Metadata management searches tags, properties, and schema of CDAP datasets, streams, and stream views.

For example, if you have just started CDAP and enabled metadata management, you could enter a search
term such as ``a* k*``, which will find all entities that begin with the letter ``a`` or
``k``.

The results would appear similar to this:

.. figure:: /_images/metadata/tracker-first-search.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

In this example, metadata management has found two datasets that satisfied the condition. The search
used is shown in the upper-left, and the results show the datasets found with
information and links for each.

**On the left side** is the **Filter** pane, which provides information on what was found (the
entities and metadata types) with statistics of the number found for each category. A
checkbox allows you to filter based on these attributes. If you mouse over a category, an
``only`` link will appear, which allows you to select *only* that category as a filter.

Note that the *entities* and *metadata* filters have an ``and`` relationship; at least one
selection must be made in each of *entities* and *metadata* for there to be any results
that appear.

**On the right side** is a sortable list of results. It is sortable by either *Create Date* or the entity
ID (name).

Each entry in the list provides a summery of information about the entity, and its name is
a hyperlink to further details: metadata and lineage.

The **Jump** button provides three actions: go to the selected entity in CDAP, or add it
to a new CDAP pipeline as a source or as a sink. Datasets can be added as sources or
sinks to batch pipelines, while streams can be sources in batch pipelines or sinks in
real-time pipelines.

Entity Details
==============
Clicking on a name in the search results list will take you to details for a particular
entity. Details are provided on the tabs *Metadata* and *Lineage*.

**Metadata**

The *Metadata* tab provides lists of the *System Tags*, *Business Tags*, *Schema*, *Business
Properties*, and *System Properties* that were found for the entity. The values shown will
vary depending on the type of entity and each individual entity. For instance, a stream
may have a schema attached, and if so, it will be displayed.

.. figure:: /_images/metadata/tracker-metadata.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

You can add business tags to any entity by clicking the plus button in the UI. You can also
remove tags by hovering over the tag and clicking the x. You can also add and remove Business
Properties for the dataset or stream. This is useful for storing additional details about
the dataset for others to see.

**Lineage**

The *Lineage* tab shows the relationship between an entity and the programs that are
interacting with it. As different lineage diagrams can be created for the same entity,
depending on the particular set of programs selected to construct the diagram, a blue
button in the shape of an arrow is used to cycle through the different lineage diagrams
that a particular entity participates in.

A date menu in the left side of the diagram lets you control the time range that the
diagram displays. By default, the last seven days are used, though a custom range can be
specified, in addition to common time ranges (two weeks to one year).

.. figure:: /_images/metadata/tracker-lineage.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image
