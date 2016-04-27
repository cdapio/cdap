.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cdap-odbc:

================
CDAP ODBC Driver
================

Overview
========

CDAP ODBC Drivers enable integration with Windows-based applications that support an ODBC
interface, allowing access to CDAP Datasets. CDAP provides ODBC drivers that support both 32-bit and
64-bit Windows operating systems. 

+-----------------------+-----------+---------------------+
| ODBC Driver           | Version   | CDAP Versions       |
+=======================+===========+=====================+
| Windows 32-bit driver | 1.0.0.1   | CDAP 3.4 and higher |
+-----------------------+-----------+---------------------+
| Windows 64-bit driver | 1.0.0.1   | CDAP 3.4 and higher |
+-----------------------+-----------+---------------------+

Installation
============
The ODBC drivers can be downloaded from these locations: 

- `Windows 32-bit driver installer <https://repository.cask.co/downloads/cdap-odbc/CDAP.ODBCDriver-x86-1.0.0.1.msi>`__
- `Windows 64-bit driver installer <https://repository.cask.co/downloads/cdap-odbc/CDAP.ODBCDriver-x64-1.0.0.1.msi>`__

After downloading the appropriate *msi* installer for your operating system and starting it,
follow the prompts to install the driver:

  .. image:: _images/odbc/odbc-00.png
     :width: 5in

..

  .. image:: _images/odbc/odbc-01.png
     :width: 5in

System restart will be required after the installation is successful for the drivers to load:

  .. image:: _images/odbc/odbc-02.png
     :width: 4in

Once the driver is installed, data sources with either CDAP Datasets 32-bit or CDAP Datasets 64-bit will be created.


Accessing CDAP Datasets from Microsoft Excel
============================================
CDAP Datasets can be accessed via the Microsoft Query Wizard in Microsoft Excel. To access
the datasets in a given namespace and to run queries against them, follow these steps:

1. From the *Data Tab* of Microsoft Excel, click on *Get External Data* -> *From Other
   Sources* -> *From Microsoft Query*:

     .. image:: _images/odbc/odbc-03.png
        :width: 6in
        :class: bordered-image
      
#. In the next dialog box, enter the CDAP router hostname, port, auth token (if
   perimeter security is enabled; optional) and the CDAP namespace:

      .. image:: _images/odbc/odbc-04.png
        :width: 4in

#. Click on *Options* and ensure the *Tables* is checked:

      .. image:: _images/odbc/odbc-05.png
        :width: 4in

#. A list of CDAP datasets and streams will be displayed in the next dialog; choose the tables
   to be queried:

    .. image:: _images/odbc/odbc-06.png
      :width: 4in

#. Choose either the columns to be fetched or to run a SQL query to fetch the data:

    .. image:: _images/odbc/odbc-07.png
      :width: 6in


Accessing CDAP Datasets from Tableau
====================================
Interactive data analytics can be performed on CDAP Datasets using `Tableau software <http://www.tableau.com>`__
by accessing the data with the CDAP ODBC driver. To use ODBC drivers on Tableau, follow these steps:

1. Choose the *Other Databases (ODBC)* option as the method to connect to the CDAP server:

    .. image:: _images/odbc/odbc-08.png
      :width: 6in
      :class: bordered-image

#. Choose the correct version of the ODBC driver in the DSN radio button:

    .. image:: _images/odbc/odbc-09.png
      :width: 4in

#. Provide CDAP router host, port, auth token (if perimeter security is enabled in
   CDAP; optional) and the CDAP namespace:

    .. image:: _images/odbc/odbc-10.png
      :width: 4in

#. Look for the CDAP datasets using the *Search* option provided by Tableau:

    .. image:: _images/odbc/odbc-11.png
      :width: 6in
      :class: bordered-image

#. Choose the desired table and column and click on the *Update Now* button to see the
   data for a particular dataset:

    .. image:: _images/odbc/odbc-12.png
      :width: 6in
      :class: bordered-image

    .. image:: _images/odbc/odbc-13.png
      :width: 6in
      :class: bordered-image-top-margin
