.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cdap-apps-etl-upgrade:

=====================
ETL Upgrade Procedure
=====================

If you wish to upgrade ETL applications created using the 3.2.x versions of
``cdap-etl-batch`` or ``cdap-etl-realtime``, you can use the ETL upgrade tool packaged
with the distributed version of CDAP. You would want to run this tool to upgrade
applications that were created with earlier versions of the artifacts, that you would
like to open in the |version| version of Cask Hydrator Studio.

The tool will connect to an instance of CDAP, look for any applications that use 3.2.x
versions of the ``cdap-etl-batch`` or ``cdap-etl-realtime`` artifacts, and then update the
application to use the |version| version of those artifacts. CDAP must be running when you
run the command:

.. parsed-literal::
  java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u \http://<host>:<port> upgrade

You can also upgrade just the ETL applications within a specific namespace:

.. parsed-literal::
  java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u \http://<host>:<port> -n <namespace> upgrade

You can also upgrade just one ETL application:

.. parsed-literal::
  java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u \http://<host>:<port> -n <namespace> -p <app-name> upgrade

If you have authentication turned on, you also need to store an authentication token in a file and pass the file to the tool:

.. parsed-literal::
  java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u \http://<host>:<port> -a <tokenfile> upgrade

