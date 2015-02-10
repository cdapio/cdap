.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

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

Identifying Entities
====================
The ID of an entity is composed of a combination of the namespace ID plus the entity ID,
since an entity cannot exist independently of a namespace.


Requirements

R1: Administrators should be able to create new namespaces.

R2: Administrators should be able to delete existing namespaces.

R3: Users should be able to view existing namespaces.

R4: We should be able to store the following metadata for namespaces: id, name,
description. This metadata should be stored in a persistent store.

R5: There should be a “default” namespace. All applications and data that a user deploys
or accesses—without explicitly specifying a namespace—will exist in the default namespace.

R6: Namespaces should be flat. There should not be any hierarchy in namespaces.

R7: Namespaces should achieve complete isolation of applications and data.
Partitioning/isolation should be clearly reflected in every entity, from application
metadata to actual datasets in storage. Specific requirements for each entity are listed
in subsequent sections in this document.


Assumptions


A1: This document assumes the existence of role-based authorization (or the existence of
an admin role to manage/administer a namespace).

A2: User management is separate from namespaces. This design will (should) not include
membership of namespaces.

A3: Every client request to a REST endpoint associated with an entity must include the
namespace either in the REST path or in the headers.

A4: A namespace is never inferred, there is no business logic to infer a namespace from
some input. A namespace is always present in the client request, and is used as-is.


First Versions and Limitations
==============================
The first version of namespaces was introduced in CDAP v2.8.0, and is part of the
HTTP RESTful API v3.

For this first version, these are out-of-scope:

- Inter-namespace operations: for example, an application from one namespace using datasets in a different namespace;
- Moving applications and/or data from one namespace to another;
- Hierarchical namespaces; and
- Quota management based on namespaces.

Design

The namespace system consists of four main components:

Namespace Admin API
Namespace Metadata
Namespace-aware Entities
