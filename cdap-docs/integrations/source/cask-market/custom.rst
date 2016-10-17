.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-market-custom:
============================
Hosting a Custom Cask Market
============================

The Cask Market APIs are simply a contract about the directory structure of the marketplace.
The directory structure must be::

  <base>/v1/packages.json
  <base>/v1/packages/<package-name>/<version>/icon.png
  <base>/v1/packages/<package-name>/<version>/spec.json
  <base>/v1/packages/<package-name>/<version>/spec.json.asc
  <base>/v1/packages/<package-name>/<version>/<resource1>
  <base>/v1/packages/<package-name>/<version>/<resource1>.asc
  <base>/v1/packages/<package-name>/<version>/<resource2>
  <base>/v1/packages/<package-name>/<version>/<resource2>.asc
  ...

As such, hosting a custom market can be done by setting up a server that follows the same
path structure.

One possible setup is to keep your packages in a source control repository whose directory
structure matches the one required by the Market. The repository can be checked out onto one or
more machines, with an Apache server configured to serve content from that directory. A tool
can be run to create the catalog file from all the package specifications, and to create all
the signature files.

Another possible setup is to serve the market catalog and packages from S3.

