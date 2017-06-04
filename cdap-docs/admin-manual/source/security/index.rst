.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

:hide-toc: true

.. _admin-security:
.. _configuration-security:
.. _security-index:

========
Security
========

.. toctree::

    Perimeter Security <perimeter-security>
    Authorization <authorization>
    Impersonation <impersonation>
    Enabling SSL for System Services <system-services>
    Secure Storage <secure-storage>

Cask Data Application Platform (CDAP) supports securing clusters using various mechanisms such as
:ref:`Perimeter Security <admin-perimeter-security>`,
:ref:`Authorization <admin-authorization>`,
:ref:`Impersonation <admin-impersonation>`,
:ref:`Enabling SSL for System Services <admin-security-system-services>`, and
:ref:`Secure Storage <admin-secure-storage>`.
This section covers how to setup these security mechanisms on a secure CDAP instance.

Additional security information, including client APIs, the authentication process,
developing authorization extensions, and authorization policies is covered in the
:ref:`Developer Manual's <developer:developer-index>` :ref:`developer:security-index` section.

.. NOTE: INCLUDED IN OTHER FILES
.. _admin-security-summary-start:

We recommend that in order for CDAP to be secure, CDAP security should always be used in conjunction with
`secure Hadoop clusters <http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SecureMode.html>`__.
In cases where secure Hadoop is not or cannot be used, it is inherently insecure and any applications
running on the cluster are effectively "trusted”. Although there is still value in having perimeter security,
authorization enforcement and secure storage in that situation, whenever possible a secure Hadoop
cluster should be employed with CDAP security.

.. _admin-security-summary-end:

CDAP Security is configured in the files ``cdap-site.xml`` and ``cdap-security.xml``:

* ``cdap-site.xml`` has non-sensitive information, such as the type of authentication, authorization and secure storage
  mechanisms, and their configuration.
* ``cdap-security.xml`` is used to store sensitive information such as keystore passwords and
  SSL certificate keys. It should be owned and readable only by the CDAP user.

These files are shown in :ref:`appendix-cdap-site.xml` and :ref:`appendix-cdap-security.xml`.

File paths shown in this section are either absolute paths or, in the case of :ref:`CDAP Local Sandbox
<local-sandbox-index>`, can be relative to the CDAP Local Sandbox installation directory.


- :ref:`admin-perimeter-security`

..

- :ref:`admin-authorization`

..

- :ref:`admin-impersonation`

..

- :ref:`admin-security-system-services`

..

- :ref:`admin-secure-storage`
