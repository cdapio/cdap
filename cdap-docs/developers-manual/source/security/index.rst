.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

:hide-toc: true

.. _security-index:

========
Security
========

.. toctree::
   :maxdepth: 1

   client-authentication
   cdap-authentication-clients-java
   cdap-authentication-clients-python
   custom-authentication
   authorization-extensions


Cask Data Application Platform (CDAP) supports securing clusters using perimeter
security. With perimeter security, cluster nodes can communicate with each other,
but outside clients can only communicate with the cluster through a secured host. 

Using CDAP security, the CDAP authentication server issues credentials (access tokens) to
authenticated clients. Clients then send these credentials on requests to CDAP. Only calls
with valid access tokens will be accepted, rejecting access to un-authenticated clients.
In addition, :ref:`access logging can be enabled <enable-access-logging>` in CDAP to
provide an audit log of all operations.


.. rubric:: Configuring Security

Security configuration is covered in the Administration Manual's :ref:`configuration-security`.


.. rubric:: Client Authentication

:ref:`client-authentication` covers:

- Authentication Process
- Supported Authentication Mechanisms
- Obtaining an Access Token
- Authentication with RESTful Endpoints


.. _authentication-clients:

.. rubric:: Authentication Client Libraries

Two authentication client libraries are included with CDAP: 

.. |cdap-authentication-clients-java| replace:: **CDAP Authentication Client for Java**
.. _cdap-authentication-clients-java: cdap-authentication-clients-java.html

.. |cdap-authentication-clients-python| replace:: **CDAP Authentication Client for Python**
.. _cdap-authentication-clients-python: cdap-authentication-clients-python.html

- |cdap-authentication-clients-java|_ 

- |cdap-authentication-clients-python|_ 


.. rubric:: Custom Authentication

If the standard authentication mechanisms are not sufficient, you can provide a
:ref:`custom authentication mechanism <developers-custom-authentication>`.

.. rubric:: Authorization Extensions

:doc:`Authorization Extensions: <authorization-extensions>` Authorization backends for CDAP
are implemented as extensions. Extensions run in their own, isolated classloader so that
there are no conflicts with the system classloader of CDAP Master.


.. impersonation-start

.. rubric:: Impersonation

Impersonation allows users to run programs and access datasets, streams, and other
resources as pre-configured users (a *principal*). Currently, CDAP supports configuring
impersonation at a namespace and at an application level, with application level
configuration having a higher precedence than namespace level.

If impersonation is enabled, and you don't specify a principal for an application, stream,
or dataset, then the namespace owner's principal is used. If there is no namespace owner
or you are using the default namespace, then the default principal is used (as set by the
properties ``cdap.master.kerberos.keytab`` and ``cdap.master.kerberos.principal`` in the
``cdap-site.xml``).

Details on the system configurations required are in the Administration Manual's section
on :ref:`admin-impersonation`.

.. impersonation-end
