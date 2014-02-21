:orphan:

.. _plugin-reference:


.. index::
   single: Security
===================
Security
===================

At Continuuity, we take security very seriously and invest a lot of time to make sure we secure communication between 
different aspects of any system that we build. Loom beta release does not include a lot of much needed security features, 
but upcoming releases of Loom will make it secure and more reliable for provisioning clusters. This document describes how 
we are planning to secure different aspects of Loom.

.. figure:: security-diagram.png
    :align: right
    :width: 800px
    :alt: Loom Security Diagram
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
