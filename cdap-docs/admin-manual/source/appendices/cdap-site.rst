.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _appendix-cdap-default.xml:
.. _appendix-cdap-site.xml:

=========================================
Appendix: cdap-site.xml, cdap-default.xml
=========================================

The ``cdap-site.xml`` file is the configuration file for a CDAP installation.
Its properties and values determine the settings used by CDAP when starting and operating.

Any properties not found in an installation's ``cdap-site.xml`` will use a default
parameter value defined in the file ``cdap-default.xml``. It is located in the CDAP JARs,
and should not be altered.

Any of the default values (with the exception of those marked :ref:`[Final]
<cdap-site-xml-note-final>`) can be over-ridden by defining a modifying value in the
``cdap-site.xml`` file, located (by default) either in ``<CDAP-SDK-HOME>/conf/cdap-site.xml`` 
(Standalone mode) or ``/etc/cdap/conf/cdap-site.xml`` (Distributed mode).

The section below are the parameters that can be defined in the ``cdap-site.xml`` file, their default
values (obtained from ``cdap-default.xml``) and their descriptions. 

.. rubric:: Notes

.. _cdap-site-xml-note-final:

- **[Final]:** Properties marked as *[Final]* indicates that their value cannot be changed, even
  with a setting in the ``cdap-site.xml``.

- **Kafka Server:** All properties that begin with ``kafka.server.`` are passed to the CDAP
  Kafka service when it is started up.

- **Security:** For information on configuring the ``cdap-site.xml`` file, its
  :ref:`security section <appendix-cdap-default-security>`, and CDAP for security, see the
  documentation :ref:`admin-security` section.


.. include:: ../../target/_includes/cdap-default-table.rst

..
.. Deprecated Properties
..

.. include:: ../../target/_includes/cdap-default-deprecated-table.rst
      :end-before: .. list-table::

These properties are deprecated as of CDAP |release| and should not be used. They
will be removed in a future release. Replacement properties are listed as noted.

.. include:: ../../target/_includes/cdap-default-deprecated-table.rst
      :start-after: ---------------------
