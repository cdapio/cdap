.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-market-custom:

============================
Hosting a Custom Cask Market
============================

.. highlight:: console

The Cask Market APIs are simply a contract about the directory structure of the marketplace.

.. Directory structure
.. --------------------
.. include:: api.rst
    :start-after: .. directory-structure-start
    :end-before: .. directory-structure-end

As such, hosting a custom market can be done by setting up a server that follows the same
path structure.

One possible setup is to keep your packages in a source control repository whose directory
structure matches the one required by the market. The repository can be checked out onto one or
more machines, with an Apache server configured to serve content from that directory. A tool
can be run to create the catalog file from all the package specifications, and to create all
the signature files.

Another possible setup is to serve the market catalog and packages from Amazon S3.
