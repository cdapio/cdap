.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

===========
Cask Market
===========

.. rubric:: Overview

The Cask Market allows CDAP users to create and update CDAP artifacts, applications,
and datasets using simple wizards. Instead of building code, deploying artifacts,
and configuring applications, users can simply point and click.
This allows users of varying technical skills to deploy and run common use
cases in a self-service manner.

The Cask Market allows system administrators to distribute re-usable applications, data, and code
to all CDAP users in their organization. Though there is currently no way to publish packages to the public
Cask hosted market, administrators can host their own market, then configure their CDAP
instances to use their own market instead of the public Cask Market.

.. rubric:: Terminology

package - A collection of entities (artifacts, applications, datasets, streams, configuration) to add to CDAP.
A package is identified by a name and version, and can be tagged with one or more categories. Each package
must contain a specification, and may contain resources.

package specification - A package specification define metadata about the package, such as a display label,
description, creation time, and CDAP compatibilities. It also contains a list of actions that must be
performed to install the package. Each action corresponds to a wizard in the installation process.

package resources - A package resource is a file that can be used during the installation process. It can
be a configuration file, a jar, or data that should be loaded into a dataset. Resources are referenced
by name in the actions defined in the package specification.

catalog - The catalog is a list of all packages in the CDAP market. The catalog contains metadata
about each package.

.. rubric:: Architecture

The Cask market is a service that is separate from a CDAP instance. Each CDAP UI instance can be configured
to read from a separate Cask market instance. By default, the UI points to the public Cask hosted market.
During package installation, the UI will make calls to the CDAP Market and to a CDAP Router instance to
create or update various CDAP entities. For example, a package may contain an action to create a stream,
then load data to a stream. The market UI will interact with the CDAP RESTful APIs to first create a stream,
then fetch the data from the CDAP market and add it to the CDAP stream through the CDAP RESTful APIs.

The market is essentially just a server that serves static package specifications and resources.
As such, administrators can easily set up their own markets in the same way they would serve any
static content. For example, an Apache web server can be placed on top of a local directory structure
that matches the expected market directory structure.

