:orphan:

.. _plugin-reference:


.. index::
   single: Security
===================
Security
===================

Below are a list of security features that will be enabled in future releases.

.. figure:: security-diagram.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center



Communication Channels
======================

 * 1 - Browser to Loom UI (Node.js)
  * Uses TLS/SSL
 * 2 - User REST APIs
  * Uses TLS/SSL
  * User REST APIs take in a user ID and a token in the headers. The token is used for authentication and user ID is used for authorization.
 * 3 - Loom Server to Database
  * Uses TLS/SSL
  * We recommend firewalling databases
 * 4 - Loom Server to Zookeeper
  * SASL
  * We recommend firewalling Zookeeper
 * 5 - Loom Server to Provisioners
  * Uses mutual authentication with TLS/SSL
 * 6 - Provisioners to Providers
  * Provider specific security settings
 * 7 - Provisioners to Nodes
  * SSH

Data Stores
===========

 * Zookeeper
  * Kerberos

 * Database
  * Setup permissions so only loom user from Loom server hosts can read/write from the database.
  * Encryption of sensitive data


Loom Components
===============

 * Loom Server
  * Database password encryption in configuration file
 * Loom Provisioner
  * Encryption of provider credentials
  * Whitelisting of shell provisioner commands
 * Loom UI
  * XSS protection
  * CSRF protection