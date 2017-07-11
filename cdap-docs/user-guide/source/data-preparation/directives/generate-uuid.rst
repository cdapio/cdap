.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=============
Generate UUID
=============

The GENERATE-UUID directive generates a universally unique identifier
(UUID) of the record.

Syntax
------

::

    generate-uuid <column>

The ``<column>`` is set to the UUID generated for the record.

Usage Notes
-----------

The GENERATE-UUID directive generates a type 4, pseudo-randomly
generated UUID. The UUID is generated using a cryptographically strong
pseudo-random number generator.

Example
-------

Using this record as an example, where you would like to generate a
random identifier for the record to uniquely identify it:

::

    {
      "x": 1,
      "y": 2
    }

Applying this directive:

::

    generate-uuid uuid

would result in a record similar to this (the value of ``uuid`` will
vary):

::

    {
      "x": 1,
      "y": 2,
      "uuid": "57126d32-8c91-4c00-9697-8abda450e836"
    }
