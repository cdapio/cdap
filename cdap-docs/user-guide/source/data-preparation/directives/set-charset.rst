.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===========
Set Charset
===========

The SET-CHARSET directive sets the encoding of the current data and then
converts it from that to a UTF-8 string.

Syntax
------

::

     set-charset <column> <charset>

-  ``column`` is the name of the column to be converted
-  ``charset`` is the charset to be used in converting the column

Usage Notes
-----------

This directive sets the character set of ``column`` to ``charset``. It
decodes the column using that ``charset`` and converts it to a UTF-8
String.
