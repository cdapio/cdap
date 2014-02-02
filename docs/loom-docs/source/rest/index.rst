:orphan:

.. _rest-api-reference:
.. include:: /toplevel-links.rst

========
REST Web Service Interface
========

The Loom REST API allows you to you fill REST APIs to interact with Loom system from 
adminstrative and user perspective. You can pretty much do everything that a UI can 
do using the these REST interfaces. 

Since the API is based on REST principles, it's very easy to write and test applications. 
You can use your browser to access URLs, and you can use pretty much any http client in 
any programming language to interact with the API

Base URL
========

All URLs referenced in the documentation have the following base:
::
 http://<loom-server>:55054/v1/loom

The Loom REST API is served over HTTP. In near future Loom APIs will be served on HTTPS to ensure data privacy, 
and unencrypted HTTP will not be supported.


Subresources
=============

Administration APIs
^^^^^^^^^^^^^^^^^^^

**Provider**
--------
  * :ref:`Create a Provider <provider-create>`
  * :ref:`View a Provider <provider-retrieve>`
  * :ref:`Delete a Provider <provider-delete>`
  * :ref:`Update a Provider <provider-modify>`
  * :ref:`View all Providers <provider-all-list>`

**Hardware**
--------
  * :ref:`Create a Hardware type <hardware-create>`
  * :ref:`View a Hardware type <hardware-retrieve>`
  * :ref:`Delete a Hardware type <hardware-delete>`
  * :ref:`Update a Hardware type <hardware-modify>`
  * :ref:`View all Hardware types <hardware-all-list>`

**Image**
-----
  * Create a Image type
  * Retrieve a Image type
  * Delete a Image type
  * Update a Image type
  * Retrieve all Images types configured

**Services**
--------
  * Create a Service
  * Retrieve a Service
  * Delete a Service
  * Update a Service
  * Retrieve all the Services

**Cluster Template**
-----------------
  * Create a Cluster template
  * Retrieve a Cluster template
  * Delete a Cluster template
  * Update a Cluster template
  * Retrieve all configured Cluster templates

User
^^^^^^^^

About REST (REpresentational State Transfer)
===============================================

We designed the Loom API in a very RESTful way, so that your consumption of it is simple and straightforward. 

From Wikipedia:

REST's proponents argue that the Web's scalability and growth are a direct result of a few key design principles:

  * Application state and functionality are divided into resources
  * Every resource is uniquely addressable using a universal syntax for use in hypermedia links
  * All resources share a uniform interface for the transfer of state between client and resource, consisting of
   * A constrained set of well-defined operations
   * A constrained set of content types, optionally supporting code on demand
  * A protocol which is:
   * Client-server
   * Stateless
   * Cacheable
   * Layered

REST's client/server separation of concerns simplifies component implementation, reduces the complexity of connector 
semantics, improves the effectiveness of performance tuning, and increases the scalability of pure server components. 
Layered system constraints allow intermediaries-proxies, gateways, and firewalls-to be introduced at various points 
in the communication without changing the interfaces between components, thus allowing them to assist in communication 
translation or improve performance via large-scale, shared caching.

REST enables intermediate processing by constraining messages to be self-descriptive: interaction is stateless between 
requests, standard methods and media types are used to indicate semantics and exchange information, and responses explicitly 
indicate cacheability.

If you're looking for more information about RESTful web services, the O'Reilly RESTful Web Services book is excellent.
