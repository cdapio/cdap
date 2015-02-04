.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _squirrel-integration:

SquirrelSQL
-----------

*SquirrelSQL* is a simple JDBC client which executes SQL queries against many different relational databases.
Here's how to add the :ref:`CDAP JDBC driver <cdap-jdbc>` inside *SquirrelSQL*.

#. Open the ``Drivers`` pane, located on the far left corner of *SquirrelSQL*.
#. Click the ``+`` icon of the ``Drivers`` pane.

   .. image:: _images/jdbc/squirrel_drivers.png
      :width: 4in

#. Add a new Driver by entering a ``Name``, such as ``CDAP Driver``. The ``Example URL`` is of the form
   ``jdbc:cdap://<host>:10000?auth.token=<token>``. The ``Website URL`` can be left blank. In the ``Class Name``
   field, enter ``co.cask.cdap.explore.jdbc.ExploreDriver``.
   Click on the ``Extra Class Path`` tab, then on ``Add``, and put the path to ``co.cask.cdap.cdap-explore-jdbc-<version>.jar``.

   .. image:: _images/jdbc/squirrel_add_driver.png
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

   .. image:: _images/jdbc/squirrel_add_alias.png
      :width: 6in

#. Click on ``OK``. ``CDAP Standalone`` is now added to the list of aliases.
#. A popup asks you to connect to your newly-added alias. Click on ``Connect``, and *SquirrelSQL* will retrieve
   information about your running CDAP Datasets.
#. To execute a SQL query on your CDAP Datasets, go to the ``SQL`` tab, enter a query in the center field, and click
   on the "running man" icon on top of the tab. Your results will show in the bottom half of the *SquirrelSQL* main view.

   .. image:: _images/jdbc/squirrel_sql_query.png
      :width: 6in

