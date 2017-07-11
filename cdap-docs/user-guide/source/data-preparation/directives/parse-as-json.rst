.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=============
Parse as JSON
=============

The PARSE-AS-JSON directive is for parsing a JSON object. The directive
can operate on String or JSONObject types. When the directive is
applied, the high-level keys of the JSON are appended to the original
column name to create new column names.

Syntax
------

::

    parse-as-json <column-name> [<depth>]

-  ``<column-name>`` is the name of the column in the record that is a
   JSON object
-  ``<depth>`` indicates the depth at which JSON object enumeration
   terminates

Usage Notes
-----------

The PARSE-AS-JSON directive breaks down complex JSON into simpler
understandable and manageable chunks. When first applied on a JSON
object, it breaks it down into keys and values. The value could in
itself be a JSON object on which you can apply the PARSE-AS-JSON
directive again to flatten it out further.

The key names in the event object are appeneded to the column that is
being applied JSON parsing. The column names use dot notation.

Examples
--------

Using this record as an example, in a field ``body``:

::

    {
      "id": 1,
      "name": {
        "first": "Root",
        "last": "Joltie"
      },
      "age": 22,
      "weigth": 184,
      "height": 5.8
    }

Applying this directive:

::

    parse-as-json body

would result in this record:

+-------------------+---------------------------------------------+--------------+
| Field Name        | Field Values                                | Field Type   |
+===================+=============================================+==============+
| ``body``          | ``{ ... }``                                 | String       |
+-------------------+---------------------------------------------+--------------+
| ``body_id``       | 1                                           | Integer      |
+-------------------+---------------------------------------------+--------------+
| ``body_name``     | ``{ "first": "Root", "last": "Joltie" }``   | JSONObject   |
+-------------------+---------------------------------------------+--------------+
| ``body_age``      | 22                                          | Integer      |
+-------------------+---------------------------------------------+--------------+
| ``body_weight``   | 184                                         | Integer      |
+-------------------+---------------------------------------------+--------------+
| ``body_height``   | 5.8                                         | Double       |
+-------------------+---------------------------------------------+--------------+

Applying the same directive, but just on the field ``body_name`` would
result in this record:

+-----------------------+---------------------------------------------+--------------+
| Field Name            | Field Values                                | Field Type   |
+=======================+=============================================+==============+
| ``body``              | ``{ ... }``                                 | String       |
+-----------------------+---------------------------------------------+--------------+
| ``body_id``           | 1                                           | Integer      |
+-----------------------+---------------------------------------------+--------------+
| ``body_name``         | ``{ "first": "Root", "last": "Joltie" }``   | JSONObject   |
+-----------------------+---------------------------------------------+--------------+
| ``body_name_first``   | Root                                        | String       |
+-----------------------+---------------------------------------------+--------------+
| ``body_name_last``    | Joltie                                      | String       |
+-----------------------+---------------------------------------------+--------------+
| ``body_age``          | 22                                          | Integer      |
+-----------------------+---------------------------------------------+--------------+
| ``body_weight``       | 184                                         | Integer      |
+-----------------------+---------------------------------------------+--------------+
| ``body_height``       | 5.8                                         | Double       |
+-----------------------+---------------------------------------------+--------------+
