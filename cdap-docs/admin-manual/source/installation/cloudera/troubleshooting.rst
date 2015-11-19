.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cloudera-troubleshooting:

=========================
Cloudera: Troubleshooting
=========================


.. rubric:: Permissions Errors

Some versions of Hive may try to create a temporary staging directory at the table
location when executing queries. If you are seeing permissions errors when running a
query, try setting ``hive.exec.stagingdir`` in your Hive configuration to
``/tmp/hive-staging``. 

This can be done in Cloudera Manager using the *Hive Client
Advanced Configuration Snippet (Safety Valve) for hive-site.xml* configuration field.


.. rubric:: Missing System Artifacts

The bundled system artifacts are included in the CDAP parcel, located in a subdirectory
of Cloudera's ``${PARCELS_ROOT}`` directory, for example::

  /opt/cloudera/parcels/CDAP/master/artifacts

Ensure that the ``App Artifact Dir`` configuration option points to this path on disk. Since this
directory can change when CDAP parcels are upgraded, users are encouraged to place
these artifacts in a static directory outside the parcel root, and configure accordingly.


.. _cloudera-direct-parcel-access:

.. rubric:: Direct Parcel Access

If you need to download and install the parcels directly (perhaps for a cluster that does
not have direct network access), the parcels are available by their full URLs. As they are
stored in a directory that does not offer browsing, they are listed here:

.. parsed-literal::
  |http:|//repository.cask.co/parcels/cdap/latest/CDAP-|version|-1-el6.parcel
  |http:|//repository.cask.co/parcels/cdap/latest/CDAP-|version|-1-precise.parcel
  |http:|//repository.cask.co/parcels/cdap/latest/CDAP-|version|-1-trusty.parcel
  |http:|//repository.cask.co/parcels/cdap/latest/CDAP-|version|-1-wheezy.parcel
  
If you are hosting your own internal parcel repository, you may also want the
``manifest.json``:

.. parsed-literal::
  |http:|//repository.cask.co/parcels/cdap/latest/manifest.json

The ``manifest.json`` can always be referred to for the list of latest available parcels.

Previously released parcels can also be accessed from their version-specific URLs.  For example:

.. parsed-literal::
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-el6.parcel
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-precise.parcel
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-trusty.parcel
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-wheezy.parcel
  

.. _cloudera-troubleshooting-upgrade-cdh:

.. rubric:: Problems While Upgrading CDH

If you miss a step in the upgrade process and something goes wrong, it's possible that the
tables will get re-enabled before the coprocessors are upgraded. This could cause the
regionservers to abort and may make it very difficult to get the cluster back to a stable
state where the tables can be disabled again and complete the upgrade process.

.. highlight:: xml

In that case, set this configuration property in ``hbase-site.xml``::

  <property>
    <name>hbase.coprocessor.abortonerror</name>
    <value>false</value>
  </property>

and restart the HBase regionservers. This will allow the regionservers to start up
despite the coprocessor version mismatch. At this point, you should be able to run through
the upgrade steps successfully. 

At the end, remove the entry for ``hbase.coprocessor.abortonerror`` in order to ensure
that data correctness is maintained.
