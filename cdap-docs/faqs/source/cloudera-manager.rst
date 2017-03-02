.. meta::
    :author: Cask Data, Inc.
    :description: Frequently Asked Questions about the Cask Data Application Platform
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

:titles-only-global-toc: true

.. _faqs-cloudera-manager:

.. highlight:: console

======================
FAQs: Cloudera Manager
======================

CDAP installed on CDH using Cloudera Manager gives a "No parcel" error |---| what do I do?
------------------------------------------------------------------------------------------
If, when you try to start services, you receive an error in ``stderr`` such as::
       
  Error found before invoking supervisord: No parcel provided required tags: set([u'cdap'])

The error message shows that a required parcel isn't available, suggesting that you
have not completed the last step of installing a parcel, *Activation*. There are 4 steps
to installing a parcel:

- **Adding the repository** to the list of repositories searched by Cloudera Manager
- **Downloading** the parcel to the Cloudera Manager server
- **Distributing** the parcel to all the servers in the cluster
- **Activating** the parcel

Start by clicking on the parcel icon (near the top-left corner of Cloudera Manager and looks
like a gift-wrapped box) and ensure that the CDAP parcel is listed as *Active*.

:ref:`Detailed instructions <admin-installation-cloudera>` are available on how to install CDAP on CDH 
(`Cloudera Distribution of Apache Hadoop <http://www.cloudera.com/content/www/en-us/resources/datasheet/cdh-datasheet.html>`__) 
using `Cloudera Manager <http://www.cloudera.com/content/www/en-us/products/cloudera-manager.html>`__. 


When I run a query, I see "Permission Error" in the logs. What do I do?
-----------------------------------------------------------------------
Some versions of Hive may try to create a temporary staging directory at the table
location when executing queries. If you are seeing permissions errors when running a
query, try setting ``hive.exec.stagingdir`` in your Hive configuration to
``/tmp/hive-staging``. 

This can be done in Cloudera Manager using the *Hive Client
Advanced Configuration Snippet (Safety Valve) for hive-site.xml* configuration field.


I'm getting a message about "Missing System Artifacts". How do I fix that?
--------------------------------------------------------------------------
The bundled system artifacts are included in the CDAP parcel, located in a subdirectory
of Cloudera Manager's ``${PARCELS_ROOT}`` directory, for example::

  /opt/cloudera/parcels/CDAP/master/artifacts

Ensure that the ``App Artifact Dir`` configuration option points to this path on disk. Since this
directory can change when CDAP parcels are upgraded, users are encouraged to place
these artifacts in a static directory outside the parcel root, and configure accordingly.


.. _faqs-cloudera-direct-parcel-access:

I'd like to directly access the parcels. How do I do that?
----------------------------------------------------------
If you need to download and install the parcels directly (perhaps for a cluster that does
not have direct network access), the parcels are available by their full URLs. As they are
stored in a directory that does not offer browsing, they are listed here:

.. parsed-literal::
  |http:|//repository.cask.co/parcels/cdap/|short-version|/CDAP-|version|-1-el6.parcel
  |http:|//repository.cask.co/parcels/cdap/|short-version|/CDAP-|version|-1-el7.parcel
  |http:|//repository.cask.co/parcels/cdap/|short-version|/CDAP-|version|-1-precise.parcel
  |http:|//repository.cask.co/parcels/cdap/|short-version|/CDAP-|version|-1-trusty.parcel
  |http:|//repository.cask.co/parcels/cdap/|short-version|/CDAP-|version|-1-wheezy.parcel
  
If you are hosting your own internal parcel repository, you may also want the
``manifest.json``:

.. parsed-literal::
  |http:|//repository.cask.co/parcels/cdap/|short-version|/manifest.json

The ``manifest.json`` can always be referred to for the list of latest available parcels.

Previously released parcels can also be accessed from their version-specific URLs.  For example:

.. parsed-literal::
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-el6.parcel
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-precise.parcel
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-trusty.parcel
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-wheezy.parcel
  
To host your own parcel repository, you will need to download all the parcels,
generate a ``manifest.json``, and serve it with the parcels from a webserver. For
more information on this, see Cloudera's documentation on `parcel repositories
<https://github.com/cloudera/cm_ext/wiki/The-parcel-repository-format>`__.

.. rubric:: Ask the CDAP Community for assistance

.. include:: cdap-user-googlegroups.txt
