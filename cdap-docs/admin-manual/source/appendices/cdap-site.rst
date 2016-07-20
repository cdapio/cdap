.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _appendix-cdap-default.xml:
.. _appendix-cdap-site.xml:

====================================================
Appendix: ``cdap-site.xml`` and ``cdap-default.xml``
====================================================

The ``cdap-site.xml`` file is the configuration file for a CDAP installation.
Its properties and values determine the settings used by CDAP when starting and operating.

Any properties not found in an installation's ``cdap-site.xml`` will use a default
parameter value defined in the file ``cdap-default.xml``. It is located in the CDAP JARs,
and should not be altered.

Any of the default values (with the exception of those marked :ref:`[Final]
<cdap-site-xml-note-final>`) can be over-ridden by defining a modifying value in the
``cdap-site.xml`` file, located (by default) either in ``<CDAP-SDK-HOME>/conf/cdap-site.xml`` 
(Standalone mode) or ``/etc/cdap/conf/cdap-site.xml`` (Distributed mode).

Below are the parameters that can be defined in the ``cdap-site.xml`` file, their default
values (obtained from ``cdap-default.xml``), descriptions, and notes. 

For information on configuring the ``cdap-site.xml`` file and CDAP for security,
see the :ref:`admin-security` section.

.. include:: ../../target/_includes/cdap-default-table.rst

.. _cdap-site-xml-note-final:

Notes
-----
**[Final]:** Properties marked as *[Final]* indicates that their value cannot be changed, even
with a setting in the ``cdap-site.xml``.
