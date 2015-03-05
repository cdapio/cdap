.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _namespaces:

============================================
Namespaces
============================================

Overview
========
A namespace is a physical grouping of application, data and its metadata in CDAP. Conceptually,
namespaces can be thought of as a partitioning of a CDAP instance. Any application or data
(referred to here as an “entity”) can exist independently in multiple namespaces at the
same time. The data and metadata of an entity is stored independent of another instance of
the same entity in a different namespace. 

The primary motivation for namespaces in CDAP is to achieve application and data
isolation. This is an intial step towards introducing `multi-tenancy
<http://en.wikipedia.org/wiki/Multitenancy>`__ into CDAP. Use-cases that benefit from
namespaces include:

- slicing a single Hadoop Cluster into multiple namespaces to support different computing
  environments, such as development, QA, and staging; and
- slicing a single Hadoop Cluster into multiple namespaces to support multiple customers.

The first version of namespaces was introduced in :ref:`CDAP v2.8.0 <release-notes>`, and
is part of the :ref:`HTTP RESTful API v3 <http-restful-api-v3>`.

Namespace Rules
===============

A Namespace consists of a namespace identifier (the namespace ID), a display name, and a description.

Namespace IDs are composed from a limited set of characters; they are restricted to
letters (a-z, A-Z), digits (0-9), hyphens (-), and underscores (_). There is no size limit
on the length of a namespace ID nor on the number of namespaces.

The namespace IDs ``default`` and ``system`` are reserved. The ``default``
namespace, however, can be used by anyone, though like all reserved namespaces, it cannot
be deleted.

Namespaces: Independent and Non-hierarchal
------------------------------------------

Namespaces are flat, with no hierarchy inside them. (Namespaces are not allowed inside
another namespace.)

As part of the independence of namespaces, inter-namespace operations are not possible:
for example, an application from one namespace using datasets from a different namespace.
Similarly, moving applications or data from one namespace to another is not possible.

Quota management based on namespaces is also not possible.


Identifying Entities
--------------------
The ID of an entity in a namespace is composed of a combination of the namespace ID plus
the entity ID, since an entity cannot exist independently of a namespace.

Once created, namespace properties (ID, display name, description) cannot be altered. 
The only method for changing them is to delete the namespace and recreate it.

Namespace IDs have a limited set of characters allowed; they are restricted to letters (a-z,
A-Z), digits (0-9), hyphens (-), and underscores (_). There is no size limit on the
on the length of a namespace ID nor on the number of namespaces.

The namespace IDs ``cdap``, ``default``, and ``system`` are reserved. The ``default``
namespace, however, can be used by anyone, though like all reserved namespaces, it cannot
be deleted.

Using Namespaces
----------------
The best practices with using namespaces would be to create desired namespaces and use
them for all operations. Otherwise, CDAP will use the ``default`` namespace for any operations
undertaken.

.. rubric::  Examples of Using Namespaces

- All examples, starting with :ref:`Hello World <examples-hello-world>`, demonstrate using
  namespaces when using the CDAP HTTP RESTful API.
  
- The CDAP :ref:`Command Line Interface <cli>` is namespace-aware. You set the
  namespace you are currently using; the command prompt displays it as a visual reminder.

