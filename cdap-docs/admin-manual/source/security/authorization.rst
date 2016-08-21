.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-authorization:

=========================
Configuring Authorization
=========================

.. _security-enabling-authorization:

Enabling Authorization
======================
To enable authorization in :term:`Distributed CDAP <distributed cdap>`, add
these properties to ``cdap-site.xml``:


Authorization in CDAP is implemented as extensions
:ref:`Developers' Manual: Authorization Extensions <authorization-extensions>`.
In addition to the above properties, an extension may require additional properties to be
configured. Extension properties, which are also specified in ``cdap-site.xml``, begin
with the prefix ``security.authorization.extension.config``. Please see the documentation
on individual extensions such as :ref:`Integrations: Apache Sentry <apache-sentry>` for
configuring properties specific to that extension

