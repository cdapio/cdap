.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cdap-apps-etl-upgrade:

=====================
ETL Upgrade Procedure
=====================

If you wish to upgrade ETL applications created using the |previous-short-version|\.x versions
of ``cdap-etl-batch`` or ``cdap-etl-realtime``, you can use the ETL upgrade tool packaged
with the distributed version of CDAP. You would want to run this tool to upgrade
applications that were created with earlier versions of the artifacts, that you would
like to open in the |version| version of Cask Hydrator Studio.

The tool will connect to an instance of CDAP, look for any applications that use |previous-short-version|\.x
versions of the ``cdap-etl-batch`` or ``cdap-etl-realtime`` artifacts, and then update the
application to use the |version| version of those artifacts. CDAP must be running when you
run the command:

.. container:: highlight

  .. parsed-literal::
  
    |$| java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u \http://<host>:<port> -e /tmp/failedUpgrades upgrade

The first argument is the host and port for the :ref:`CDAP router
<appendix-cdap-default-router>`. The second argument is a directory to write the
configurations of any pipelines that could not be upgraded. A pipeline may fail to upgrade
if the new version of a plugin used in the pipeline is not backwards compatible. For
example, this may happen if the plugin added a new required property.

You can also upgrade just the ETL applications within a specific namespace:

.. container:: highlight

  .. parsed-literal::
  
    |$| java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u \http://<host>:<port> -n <namespace-id> upgrade

You can also upgrade just one ETL application:

.. container:: highlight

  .. parsed-literal::
  
    |$| java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u \http://<host>:<port> -n <namespace-id> -p <app-name> upgrade

If you have authentication turned on, you also need to store an access token in a file and pass the file to the tool:

.. container:: highlight

  .. parsed-literal::
  
    |$| java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u \http://<host>:<port> -a <tokenfile> upgrade

For instance, if you have obtained an access token (as shown in the example in the
`security documentation <testing-security>`) such as::

    {"access_token":"AghjZGFwAI7e8p65Uo7OpfG5UrD87psGQE0u0sFDoqxtacdRR5GxEb6bkTypP7mXdqvqqnLmfxOS",
      "token_type":"Bearer","expires_in":86400}

The access token itself (``AghjZGFwAI7e8p65Uo7OpfG5UrD87psGQE0u0sFDoqxtacdRR5GxEb6bkTypP7mXdqvqqnLmfxOS``) 
would be placed in a file and then the file's path would be used in the above command.
