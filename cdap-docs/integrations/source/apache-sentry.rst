.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _apache-sentry:

.. NOTE: Because this file includes the cdap-sentry-extension-readme.txt with its headings
..       a "+" has been used instead of "=" to push all included levels down one level.

+++++++++++++
Apache Sentry
+++++++++++++

.. _apache-sentry-configuration:

Configuring Apache Sentry for Integration with CDAP
+++++++++++++++++++++++++++++++++++++++++++++++++++
To use CDAP on a cluster using Apache Sentry for authorization, set this property:

- Add the user ``cdap`` to the Sentry property ``sentry.service.allow.connect``

We also recommend setting these properties:

- Add the user ``cdap`` to the Sentry property ``sentry.service.admin.group``; this allows
  the CDAP user to create roles, add roles to users, remove roles, list all roles, etc.

- Add the user ``cdap`` to the Hive property ``sentry.metastore.service.users``; this
  should be set if you want to allow CDAP to bypass Sentry authorization for Hive Metastore
  queries, such as the default case where all applications run as ``cdap`` and not as
  individual users.

**Note:** You must restart Apache Sentry and HiveServer2 after setting these properties.

.. _cdap-sentry-authorization-extension:

CDAP Sentry Authorization Extension
+++++++++++++++++++++++++++++++++++
In addition to setting up CDAP to work on a cluster already using Apache Sentry for
authorization of non-CDAP components, users can also use the CDAP Sentry Authorization
Extension to enforce authorization on CDAP entities using Apache Sentry.

.. include:: /../target/_includes/cdap-sentry-extension-readme.txt
    :start-line: 6
    :end-before: Share and Discuss!
