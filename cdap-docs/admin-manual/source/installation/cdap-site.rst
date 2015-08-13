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

Any of its values can be over-ridden by defining a modifying value in the
``cdap-site.xml`` file, located by default either in
``<CDAP-SDK-HOME>/conf/cdap-site.xml`` (Standalone mode) or
``/etc/cdap/conf/cdap-site.xml`` (Distributed mode).

Here are the parameters that can be defined in the ``cdap-site.xml`` file, their default
values (obtained from ``cdap-default.xml``), descriptions, and notes. 

For information on configuring the ``cdap-site.xml`` file and CDAP for security,
see the :ref:`configuration-security` section.

.. include:: ../../build/_includes/cdap-default-table.rst

.. _note 1:

**Note 1**:

    **kafka.default.replication.factor:** Used to replicate *Kafka* messages across multiple
    machines to prevent data loss in the event of a hardware failure. The recommended setting
    is to run at least two *Kafka* servers. If you are running two *Kafka* servers, set this
    value to 2; otherwise, set it to the number of *Kafka* servers.

.. _note 2:

**Note 2**:

    **thrift.max.read.buffer:** Maximum read buffer size in bytes used by the Thrift
    server; this value should be set to greater than the maximum frame sent on the RPC
    channel.
