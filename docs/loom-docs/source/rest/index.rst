.. _rest-api-reference:

.. index::
   single: Web Services
=============
Web Services
=============

REST by far is the most efficient and seamless way for disparate and distributed systems to exchange or communicate information. Most 
interactions are characterized as request-response-action-based exchange. And invariably, there is a client (requesting an action or resource)
and the server (providing the response or resource) to complete an exchange between two endpoints over HTTP protocol. In that manner, the Loom REST API, then, allows you to interact 
with the Loom system from an administrative and user perspective. You can pretty much do everything that a UI can do using these 
REST interfaces. 

Since the API is based on REST principles, it's very easy to write and test applications. You can use your browser to access URLs, 
and use pretty much any http client in any programming language of your choice to interact with the API

Base URL
========

All URLs referenced in the documentation have the following base:
::
 http://<loom-server>:<loom-port>/v1/loom

In addition, two headers must be sent to all REST endpoints.  The first is ``X-Loom-UserID`` and is used to specify
the id of the user making the request.  The second is ``X-Loom-ApiKey`` and is used to specify the api key used to
communicate with the server.

.. note:: The Loom REST API is served over HTTP. In the near future, the Loom APIs will be served on HTTPS to ensure data privacy, and unencrypted HTTP will not be supported.

Administration APIs
====================

Provider
------------
  * :ref:`Create a Provider <provider-create>`
  * :ref:`View a Provider <provider-retrieve>`
  * :ref:`Delete a Provider <provider-delete>`
  * :ref:`Update a Provider <provider-modify>`
  * :ref:`View all Providers <provider-all-list>`

Hardware
------------
  * :ref:`Create a Hardware type <hardware-create>`
  * :ref:`View a Hardware type <hardware-retrieve>`
  * :ref:`Delete a Hardware type <hardware-delete>`
  * :ref:`Update a Hardware type <hardware-modify>`
  * :ref:`View all Hardware types <hardware-all-list>`

Image
---------
  * :ref:`Create an Image type <image-create>`
  * :ref:`Retrieve an Image type <image-retrieve>`
  * :ref:`Delete an Image type <image-delete>`
  * :ref:`Update an Image type <image-modify>`
  * :ref:`Retrieve all Image types configured <image-all-list>`

Services
------------
  * :ref:`Add a Service <service-create>`
  * :ref:`Retrieve a Service <service-retrieve>`
  * :ref:`Delete a Service <service-delete>`
  * :ref:`Update a Service type <service-modify>`
  * :ref:`List all Services <service-all-list>`

Cluster Templates
--------------------
  * :ref:`Create a Cluster template <template-create>`
  * :ref:`Retrieve a Cluster template <template-retrieve>`
  * :ref:`Delete a Cluster template <template-delete>`
  * :ref:`Update a Cluster template <template-modify>`
  * :ref:`Retrieve all configured Cluster templates <template-all-list>`

Import/Export
-----------------
  * :ref:`Exporting Template Metadata <entity-export>`
  * :ref:`Importing Template Metadata <entity-import>`

User APIs
=========
The User Web service provides methods that can be used to create, delete and get status of cluster 
being created. 

Clusters
------------
  * :ref:`Creating a Cluster <cluster-create>`
  * :ref:`Getting Cluster Details <cluster-details>`
  * :ref:`Deleting a Cluster <cluster-delete>`
  * :ref:`Getting Status of a Cluster <cluster-status>`
  * :ref:`Getting a Cluster Action Plan <cluster-plan>`

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
