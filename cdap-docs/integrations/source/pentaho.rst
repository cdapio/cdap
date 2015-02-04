.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _pentaho-integration:

Pentaho Data Integration
------------------------

*Pentaho Data Integration* is an advanced, open source business intelligence tool that can execute
transformations of data coming from various sources. Let's see how to connect it to
CDAP Datasets using the CDAP JDBC driver.

#. Before opening the *Pentaho Data Integration* application, copy the ``co.cask.cdap.cdap-explore-jdbc-<version>.jar``
   file to the ``lib`` directory of *Pentaho Data Integration*, located at the root of the application's directory.
#. Open *Pentaho Data Integration*.
#. In the toolbar, select ``File -> New -> Database Connection...``.
#. In the ``General`` section, select a ``Connection Name``, like ``CDAP Standalone``. For the ``Connection Type``, select
   ``Generic database``. Select ``Native (JDBC)`` for the ``Access`` field. In this example, where we connect to
   a standalone instance of CDAP, our ``Custom Connection URL`` will then be ``jdbc:cdap://localhost:10000``.
   In the field ``Custom Driver Class Name``, enter ``co.cask.cdap.explore.jdbc.ExploreDriver``.

   .. image:: _images/jdbc/pentaho_add_connection.png
      :width: 6in

#. Click on ``OK``.
#. To use this connection, navigate to the ``Design`` tab on the left of the main view. In the ``Input`` menu,
   double click on ``Table input``. It will create a new transformation containing this input.

   .. image:: _images/jdbc/pentaho_table_input.png
      :width: 6in

#. Right-click on ``Table input`` in your transformation and select ``Edit step``. You can specify an appropriate name
   for this input such as ``CDAP Datasets query``. Under ``Connection``, select the newly created database connection;
   in this example, ``CDAP Standalone``. Enter a valid SQL query in the main ``SQL`` field. This will define the data
   available to your transformation.

   .. image:: _images/jdbc/pentaho_modify_input.png
      :width: 6in

#. Click on ``OK``. Your input is now ready to be used in your transformation, and it will contain data coming
   from the results of the SQL query on the CDAP Datasets.
#. For more information on how to add components to a transformation and link them together, see the
   `Pentaho Data Integration page <http://community.pentaho.com/projects/data-integration/>`__.
