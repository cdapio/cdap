.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _exploration-integration:

Integrating with External Systems
-------------------------------------
CDAP provides a JDBC driver to make integrations with external programs and third-party BI (business intelligence)
tools easier.

The JDBC driver is a JAR that is bundled with the CDAP SDK. You can find it in the ``lib``
directory of your SDK installation at::

  lib/co.cask.cdap.cdap-explore-jdbc-<version>.jar

If you don't have a CDAP SDK and only want to connect to an existing instance of CDAP, 
you can download the CDAP JDBC driver from `this link 
<https://repository.continuuity.com/content/groups/public/co/cask/cdap/cdap-explore/>`__.
Go to the directory matching the version of your running CDAP instance, and download the file 
with the matching version number::

  cdap-explore-jdbc-<version>.jar

Using the CDAP JDBC driver in your Java code
............................................

To use CDAP JDBC driver in your code, place ``cdap-jdbc-driver.jar`` in the classpath of your application.
If you are using Maven, you can simply add a dependency in your file ``pom.xml``::

  <dependencies>
    ...
    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-explore-jdbc</artifactId>
      <version><!-- Version of CDAP you want the JDBC driver to query --></version>
    </dependency>
    ...
  </dependencies>

Here is a snippet of Java code that uses the CDAP JDBC driver to connect to a running instance of CDAP,
and executes a query::

  // First, register the driver once in your application
  Class.forName("co.cask.cdap.explore.jdbc.ExploreDriver");

  // If your CDAP instance requires a authentication token for connection,
  // you have to specify it here.
  // Replace <cdap-host> and <authentication_token> as appropriate to your installation.
  String connectionUrl = "jdbc:cdap://<cdap-host>:10000" +
    "?auth.token=<authentication_token>";

  // Connect to CDAP instance
  Connection connection = DriverManager.getConnection(connectionUrl);

  // Execute a query over CDAP Datasets and retrieve the results
  ResultSet resultSet = connection.prepareStatement("select * from cdap_user_mydataset").executeQuery();
  ...

JDBC drivers are a standard in the Java ecosystem, with many `resources about them available
<http://docs.oracle.com/javase/tutorial/jdbc/>`__.

Accessing CDAP Datasets through Business Intelligence Tools
-----------------------------------------------------------

Most Business Intelligence tools can integrate with relational databases using JDBC drivers. They often include
drivers to connect to standard databases such as MySQL or PostgreSQL.
Most tools allow the addition of non-standard JDBC drivers.

We'll look at two business intelligence tools — *SquirrelSQL* and *Pentaho Data Integration* —
and see how to connect them to a running CDAP instance and interact with CDAP Datasets.

SquirrelSQL
...........

*SquirrelSQL* is a simple JDBC client which executes SQL queries against many different relational databases.
Here's how to add the CDAP JDBC driver inside *SquirrelSQL*.

#. Open the ``Drivers`` pane, located on the far left corner of *SquirrelSQL*.
#. Click the ``+`` icon of the ``Drivers`` pane.

   .. image:: ../_images/jdbc/squirrel_drivers.png
      :width: 4in

#. Add a new Driver by entering a ``Name``, such as ``CDAP Driver``. The ``Example URL`` is of the form
   ``jdbc:cdap://<host>:10000?auth.token=<token>``. The ``Website URL`` can be left blank. In the ``Class Name``
   field, enter ``co.cask.cdap.explore.jdbc.ExploreDriver``.
   Click on the ``Extra Class Path`` tab, then on ``Add``, and put the path to ``co.cask.cdap.cdap-explore-jdbc-<version>.jar``.

   .. image:: ../_images/jdbc/squirrel_add_driver.png
      :width: 6in

#. Click on ``OK``. You should now see ``Cask CDAP Driver`` in the list of drivers from the ``Drivers`` pane of
   *SquirrelSQL*.
#. We can now create an alias to connect to a running instance of CDAP. Open the ``Aliases`` pane, and click on
   the ``+`` icon to create a new alias.
#. In this example, we are going to connect to a standalone CDAP from the SDK.
   The name of our alias will be ``CDAP Standalone``. Select the ``CDAP Driver`` in
   the list of available drivers. Our URL will be ``jdbc:cdap://localhost:10000``. Our standalone instance
   does not require an authentication token, but if yours requires one, HTML-encode your token
   and pass it as a parameter of the ``URL``. ``User Name`` and ``Password`` are left blank.

   .. image:: ../_images/jdbc/squirrel_add_alias.png
      :width: 6in

#. Click on ``OK``. ``CDAP Standalone`` is now added to the list of aliases.
#. A popup asks you to connect to your newly-added alias. Click on ``Connect``, and *SquirrelSQL* will retrieve
   information about your running CDAP Datasets.
#. To execute a SQL query on your CDAP Datasets, go to the ``SQL`` tab, enter a query in the center field, and click
   on the "running man" icon on top of the tab. Your results will show in the bottom half of the *SquirrelSQL* main view.

   .. image:: ../_images/jdbc/squirrel_sql_query.png
      :width: 6in

Pentaho Data Integration
........................

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

   .. image:: ../_images/jdbc/pentaho_add_connection.png
      :width: 6in

#. Click on ``OK``.
#. To use this connection, navigate to the ``Design`` tab on the left of the main view. In the ``Input`` menu,
   double click on ``Table input``. It will create a new transformation containing this input.

   .. image:: ../_images/jdbc/pentaho_table_input.png
      :width: 6in

#. Right-click on ``Table input`` in your transformation and select ``Edit step``. You can specify an appropriate name
   for this input such as ``CDAP Datasets query``. Under ``Connection``, select the newly created database connection;
   in this example, ``CDAP Standalone``. Enter a valid SQL query in the main ``SQL`` field. This will define the data
   available to your transformation.

   .. image:: ../_images/jdbc/pentaho_modify_input.png
      :width: 6in

#. Click on ``OK``. Your input is now ready to be used in your transformation, and it will contain data coming
   from the results of the SQL query on the CDAP Datasets.
#. For more information on how to add components to a transformation and link them together, see the
   `Pentaho Data Integration page <http://community.pentaho.com/projects/data-integration/>`__.


Formulating Queries
-------------------
When creating your queries, keep these limitations in mind:

- The query syntax of CDAP is a subset of the variant of SQL that was first defined by Apache Hive.
- The SQL commands ``UPDATE`` and ``DELETE`` are not allowed on CDAP Datasets.
- When addressing your datasets in queries, you need to prefix the data set name with the CDAP
  namespace ``cdap_user_``. For example, if your Dataset is named ``ProductCatalog``, then the corresponding table
  name is ``cdap_user_productcatalog``. Note that the table name is lower-case.

For more examples of queries, please refer to the `Hive language manual
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries>`__.
