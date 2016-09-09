.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. _namespaces:

==========
Namespaces
==========

Overview
========
A *Namespace* is a logical grouping of application, data and its metadata in CDAP. Conceptually,
namespaces can be thought of as a partitioning of a CDAP instance. Any application or data
(referred to here as an “entity”) can exist independently in multiple namespaces at the
same time. The data and metadata of an entity is stored independent of another instance of
the same entity in a different namespace. 

The primary motivation for namespaces in CDAP is to achieve application and data
isolation. This is an intial step towards introducing `multi-tenancy
<http://en.wikipedia.org/wiki/Multitenancy>`__ into CDAP. Use-cases that benefit from
namespaces include partitioning a single Hadoop Cluster into multiple namespaces:

- to support different computing environments, such as development, QA, and staging;
- to support multiple customers; and 
- to support multiple sub-organizations within an organization.

The first version of namespaces was introduced in :ref:`CDAP v2.8.0 <release-notes>`, and
is part of the :ref:`HTTP RESTful API v3 <http-restful-api-v3>`.


Namespace Components
====================

A namespace has a namespace identifier (the namespace 'name') and a description.

Namespace IDs are composed from a limited set of characters; they are restricted to
letters (a-z, A-Z), digits (0-9), and underscores (_). There is no size limit
on the length of a namespace ID nor on the number of namespaces.

The namespace IDs ``cdap``, ``default``, and ``system`` are reserved. The ``default``
namespace, however, can be used by anyone, though like all reserved namespaces, it cannot
be deleted.


Independent and Non-hierarchical
================================

Namespaces are flat, with no hierarchy inside them. (Namespaces are not allowed inside
another namespace.)

As part of the independence of namespaces, inter-namespace operations are not possible:
for example, an application from one namespace using datasets from a different namespace.
Similarly, moving applications or data from one namespace to another is not possible.

Quota management based on namespaces is also not possible in this release, but may be a
feature in a future release.


Identifying Entities in a Namespace
===================================
The ID of an entity in a namespace is composed of a combination of the namespace ID plus
the entity ID, since an entity cannot exist independently of a namespace.


Using Namespaces
================
The best practice with using namespaces would be to create desired namespaces and use
them for all operations. Otherwise, CDAP will use the ``default`` namespace for any
operations undertaken.

Once a namespace has been created, you can edit its description and configuration
preferences, either by using a :ref:`RESTful API <http-restful-api-namespace>` or the 
:ref:`Command Line Interface <cli>`.

CDAP includes the ``default`` namespace out-of-the-box. It is guaranteed to always be
present, and is recommended for:

1. Proof-of-concept or sandbox applications for trying out CDAP; and

2. Testing your apps before deploying them in development, QA, or production environments.

It is the namespace used when no other namespace is specified. However, for most usecases
beyond the proof-of-concept stage, we recommend that you create appropriate namespaces and
operate CDAP within them.

Namespaces can be deleted. When a namespace is deleted, all components (applications,
streams, flows, datasets, MapReduce programs, metrics, etc.) are first deleted, and then
the namespace itself is removed. In the case of the ``default`` namespace, the name is
retained, as the ``default`` namespace is always available in CDAP. 

As this is an unrecoverable operation, extreme caution must be used when deleting
namespaces. It can only be done if all programs of the namespace have been stopped, and if
the ``cdap-site.xml`` parameter ``enable.unrecoverable.reset`` has been enabled.


.. _namespaces-custom-mapping:

Custom Mapping of Storage Providers
===================================
When creating a namespace, the underlying storage provider can also be configured for the
namespace. For example, a custom HBase namespace, Hive database, or HDFS directory can be
specified to be used for the data in a particular namespace.
See :ref:`Namespace Configurations <http-restful-api-namespace-configs>` for how to specify
the properties of a namespace.

When configuring an underlying storage provider, CDAP will not manage the lifecycle of these
entities; they must exist before the CDAP namespace is created, and they will not be removed
upon the deletion of the CDAP namespace. The contents (the data) of the storage provider are
deleted when a CDAP namespace is deleted through CDAP.


Namespace Examples
==================
- All examples, starting with :ref:`Hello World <examples-hello-world>`, demonstrate using
  namespaces when using the CDAP HTTP RESTful API.
  
- The CDAP :ref:`Command Line Interface <cli>` is namespace-aware. You set the
  namespace you are currently using; the command prompt displays it as a visual reminder.
