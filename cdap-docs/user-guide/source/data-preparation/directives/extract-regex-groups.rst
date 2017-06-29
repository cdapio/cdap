.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

====================
Extract Regex Groups
====================

The EXTRACT-REGEX-GROUPS directive extracts the data from a regex group
into its own column.

Syntax
------

::

    extract-regex-groups <column> <regex-with-groups>

The directive generates additional columns based on the regex in
``<regex-with-groups>``. This ignores the ``$0`` regex group.

Usage Notes
-----------

If multiple groups are matched, the directive creates multiple columns.

The base name of the column is appended with the match count and match
position the pattern is matched for:
``<column>_<match-count>_<match-position>``.

Example
-------

Using this record as an example:

::

    {
      "title": "Toy Story (1995)"
    }

Applying this directive:

::

    extract-regex-groups title [^(]+\(([0-9]{4})\).*

would result in this record:

::

    {
      "title": "Toy Story (1995)",
      "title_1_1: "1995"
    }

The field ``title_1_1`` follows the format of
``<column>_<match-count>_<match-position>``.
