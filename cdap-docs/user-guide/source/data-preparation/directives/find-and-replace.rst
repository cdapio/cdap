.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

================
Find and Replace
================

The FIND-AND-REPLACE directive transforms string column values using a
"sed"-like expression to find and replace text.

Syntax
------

::

    find-and-replace <column> <sed-script>

Usage Notes
-----------

This directive is a column-oriented text processor that operates on a
single value in the given column. The ``sed-script`` is applied on each
text value to transform the data.

A typical example on how the directive is used:

::

    find-and-replace <column> s/regex/replacement/g

This directive will replace a value of the column that matches the
``regex`` with the ``replacement`` value.

The ``s`` stands for "substitute". The ``g`` stands for "global", which
means that all matching occurrences in the value would be replaced.

The regular expression to be matched is placed after the first
delimiting symbol (a forward-slash in this example) and the replacement
value follows the second delimiting symbol. A forward-slash (``/``) is
the conventional symbol used as a delimiter, and the origin of the
character for "search".

For example, to replace all occurrences of ``hello`` with ``world`` in
the column ``message``:

::

    find-and-replace message s/hello/world/g

If you want to change a pathname that contains a slash (such as
``/usr/local/bin`` to ``/common/bin``), you can use a backslash to
escape any slashes:

::

      find-and-replace column s/\/usr\/local\/bin/\/common\/bin//g

Example
-------

Using this record as an example:

::

    {
      "body": "one two three four five six seven eight"
    }

Applying these two directives:

::

    find-and-replace body s/one/ONE/g
    find-and-replace body s/two/2/g

would result in this record:

::

    {
      "body": "ONE 2 three four five six seven eight",
    }
