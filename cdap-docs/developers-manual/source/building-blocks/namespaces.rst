.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _namespaces:

============================================
Namespaces
============================================

Overview
========
A namespace is a physical grouping of application and data in CDAP. Conceptually,
namespaces can be thought of as a partitioning of a CDAP instance. Any application or data
(referred to here as an “entity”) can exist independently in multiple namespaces at the
same time. The data and metadata of an entity is stored independent of another instance of
the same entity in a different namespace. 

The primary motivation for namespaces in CDAP is to achieve application and data
isolation. This is an intial step towards introducing `multi-tenancy
<http://en.wikipedia.org/wiki/Multitenancy>`__ into CDAP.


Namespace Rules
===============

A Namespace consists of a namespace identifier (the namespace ID), a display name, and a description.

Namespace IDs are composed from a limited set of characters; they are restricted to
letters (a-z, A-Z), digits (0-9), hyphens (-), and underscores (_). There is no size limit
on the length of a namespace ID nor on the number of namespaces.

The namespace IDs ``cdap``, ``default``, and ``system`` are reserved. The ``default``
namespace, however, can be used by anyone, though like all reserved namespaces, it cannot
be deleted.

Namespaces are flat, with no hierarchy inside them. (Namespaces are not allowed inside
another namespace.)

Identifying Entities
--------------------
The ID of an entity in a namespace is composed of a combination of the namespace ID plus
the entity ID, since an entity cannot exist independently of a namespace.


First Version Limitations
==============================
The first version of namespaces was introduced in CDAP v2.8.0, and is part of the
HTTP RESTful API v3.

For this first version, certain limitations apply and are not allowed or implemented:

- Once created, namespace properties (ID, display name, description) cannot be altered, 
  only deleted;
- Inter-namespace operations: for example, an application from one namespace using
  datasets from a different namespace;
- Moving applications and/or data from one namespace to another;
- Hierarchical namespaces; and
- Quota management based on namespaces.


.. rubric::  Examples of Using Namespaces

- All examples, starting with :ref:`Hello World <examples-hello-world>`, demonstrate using
  namespaces when using the CDAP HTTP RESTful API.
  

