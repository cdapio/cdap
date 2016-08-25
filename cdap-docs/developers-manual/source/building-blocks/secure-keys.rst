.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _secure-keys-programmatic:

===========
Secure Keys
===========
A :term:`Secure Key` is an identifier for an entry in :ref:`Secure Storage <admin-secure-storage>`. It is used to
store information such as passphrases, cryptographic keys, passwords, and other sensitive information. Users can access
and manage secure keys using the :ref:`Secure Keys HTTP RESTful API <http-restful-api-secure-storage>`. Secure keys
can also be accessed and managed from programs.

Accessing Secure Keys in Programs
=================================
Secure keys can be accessed in programs using the context object returned by the ``getContext()`` method. This object
has these methods to access secure keys:

- ``listSecureData(String namespace)``: Lists all secure keys in the specified namespace
- ``getSecureData(String namespace, String name)``: Retrieves the secure key identified by the key name ``name``
  in the specified namespace

Managing Secure Keys in Programs
================================
Secure keys can be added or deleted in programs using the ``Admin`` object available in programs as::

  Admin admin = getContext().getAdmin();

The ``Admin`` object returned by the above call exposes these methods to manage secure keys:

- ``putSecureData(String namespace, String name, String data, String description, Map<String, String> properties)``:
  Adds a new secure key identified by the key name ``name`` in the specified namespace
- ``deleteSecureData(String namespace, String name)``: Deletes the secure key (and its associated data) identified
  by the key name ``name`` in the specified namespace

For details on the setting the other parameters of the ``putSecureData()`` method, see the 
:ref:`Secure Keys HTTP RESTful API <http-restful-api-secure-storage>`.
