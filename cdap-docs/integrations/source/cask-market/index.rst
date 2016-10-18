.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

:hide-toc: true

.. _cask-market:

===========
Cask Market
===========

.. toctree::
   :maxdepth: 6
   
    Cask Market HTTP RESTful API <api>
    Hosting a Custom Cask Market <custom>

.. rubric:: Overview

The Cask Market allows CDAP users to create and update CDAP artifacts, applications, and
datasets using simple wizards. Instead of building code, deploying artifacts, and
configuring applications, users can simply point and click. This allows users of varying
technical skill the ability to deploy and run common use-cases in a self-service manner.

The Cask Market allows system administrators to distribute re-usable applications, data,
and code to all CDAP users in their organization. Though there is currently no method for
publishing packages to the public Cask-hosted market, administrators can host their own
market and then configure their CDAP instances to use their own market instead of the
public Cask Market.

.. rubric:: Terminology

**Package:** A collection of entities (artifacts, applications, datasets, streams,
configuration) to add to CDAP. A package is identified by a name and version, and can be
tagged with one or more categories. Each package must contain a specification, and may
contain resources.

**Package Specification:** Defines metadata about the package, such as display label,
description, creation time, and CDAP compatibilities. It contains a list of actions that
must be performed to install the package. Each action corresponds to a wizard in the
installation process.

**Package Resource:** A file that can be used during the installation process. It can be a
configuration file, a JAR, or data that should be loaded into a dataset. Resources are
referenced by name in the actions defined in the package specification.

**Catalog:** The catalog is a list of all the packages in the Cask Market. The catalog
contains metadata about each package.

.. rubric:: Architecture

The Cask Market is a service that is separate from a CDAP instance. Each CDAP UI instance
can be configured to read from a separate Cask Market instance. By default, the UI points
to the public Cask-hosted market. During package installation, the UI will make calls to
the Cask Market and to a CDAP Router instance to create or update various CDAP entities.
For example, a package may contain an action to create a stream, and then load data to a
stream. The Cask Market UI will interact with the CDAP RESTful APIs to create the stream,
then fetch the data from the Cask Market and add it to the CDAP stream through the CDAP
RESTful APIs.

A market is essentially just a server that serves static package specifications and
resources. As such, administrators can easily set up their own markets in the same way
they would serve any static content. For example, an Apache web server can be placed on
top of a local directory structure that matches the expected market directory structure.
