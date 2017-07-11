.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

==============
Catalog Lookup
==============

The CATALOG-LOOKUP directive provides lookups into catalogs that are
pre-loaded (static). Currently, the directive supports looking up health
care ICD-9 and ICD-10-{2016,2017} codes.

Syntax
------

::

    catalog-lookup <catalog> <column>

The ``<catalog>`` specifies the dictionary which should be used for
looking up the value in the ``<column>``.

These catalogs are currently supported:

-  ICD-9
-  ICD-10-2016
-  ICD-10-2017

Usage Notes
-----------

Using this record as an example: a record containing a single field
(``code``) that requires looking up:

::

    {
      "code": "Y36521S"
    }

Applying the CATALOG-LOOKUP directive with the ICD-10-2016 Catalog:

::

    catalog-lookup ICD-10-2016 code

would result in the record having an additional column
``code_<catalog>_description`` containing the result of the lookup. In
cases where there is no matching code, a ``null`` is stored instead. For
this example, in ``code_icd_10_2016_description``:

::

    {
      "code": "Y36521S",
      "code_icd_10_2016_description": "War operations involving indirect blast effect of nuclear weapon, civilian, sequela"
    }

In cases where the lookup is null or empty for a record, a ``null``
value is added to the ``column`` field.
