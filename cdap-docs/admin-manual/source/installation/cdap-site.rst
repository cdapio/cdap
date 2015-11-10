.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _appendix-cdap-default.xml:
.. _appendix-cdap-site.xml:

====================================================
Appendix: ``cdap-default.xml`` and ``cdap-site.xml``
====================================================

Default parameter values for configuring CDAP are defined in the file
``cdap-default.xml``. It is located in the CDAP JARs, and should not be altered.

Any of its values (with the exception of those marked :ref:`[Final] <cdap-site-xml-note-final>`)
can be over-ridden by defining a modifying value in the
``cdap-site.xml`` file, located by default either in
``<CDAP-SDK-HOME>/conf/cdap-site.xml`` (Standalone mode) or
``/etc/cdap/conf/cdap-site.xml`` (Distributed mode).

Here are the parameters that can be defined in the ``cdap-site.xml`` file, their default
values (obtained from ``cdap-default.xml``), descriptions, and notes. 

For information on configuring the ``cdap-site.xml`` file and CDAP for security,
see the :ref:`configuration-security` section.

.. include:: ../../target/_includes/cdap-default-table.rst

.. _cdap-site-xml-note-final:

Notes
-----
**[Final]:** Properties marked as *[Final]* indicates that their value cannot be changed, even
with a setting in the ``cdap-site.xml``.
