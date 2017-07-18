.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===============
Trimming Spaces
===============

The TRIM, LTRIM, and RTRIM directives trim whitespace from both sides,
left side or right side of a string values they are applied to.

Syntax
------

::

    trim <column>
    ltrim <column>
    rtrim <column>

The directive performs an in-place trimming of space in the value
specified by the ``<column>``

Usage
-----

The trim directives honors UNICODE space characters. Following are the
characters that are recognized as spaces by TRIM, LTRIM and RTRIM.

+-----------------------+-----------------------------+
| Character             | Description                 |
+=======================+=============================+
| `\t`                  | Character tabulation        |
+-----------------------+-----------------------------+
| `\n`                  | Line Feed (LF)              |
+-----------------------+-----------------------------+
| `\u000B`              | Line Tabulation             |
+-----------------------+-----------------------------+
| `\f`                  | Form Feed (FF)              |
+-----------------------+-----------------------------+
| `\r`                  | Carriage Return (CR)        |
+-----------------------+-----------------------------+
| " "                   | Space                       |
+-----------------------+-----------------------------+
| `\u0085`              | Next line (NEL)             |
+-----------------------+-----------------------------+
| `\u00A0`              | No Break Space              |
+-----------------------+-----------------------------+
| `\u1680`              | OGHAM Space Mark            |
+-----------------------+-----------------------------+
| `\u180E`              | Mongolian Vowel Separator   |
+-----------------------+-----------------------------+
| `\u2000`              | EN Quad                     |
+-----------------------+-----------------------------+
| `\u2001`              | EM Quad                     |
+-----------------------+-----------------------------+
| `\u2002`              | EN Space                    |
+-----------------------+-----------------------------+
| `\u2003`              | EM Space                    |
+-----------------------+-----------------------------+
| `\u2004`              | Three Per EM space          |
+-----------------------+-----------------------------+
| `\u2005`              | Four Per EM space           |
+-----------------------+-----------------------------+
| `\u2006`              | Six Per EM space            |
+-----------------------+-----------------------------+
| `\u2007`              | Figure Space                |
+-----------------------+-----------------------------+
| `\u2008`              | Puncatuation Space          |
+-----------------------+-----------------------------+
| `\u2009`              | Thin Space                  |
+-----------------------+-----------------------------+
| `\u200A`              | Hair Space                  |
+-----------------------+-----------------------------+
| `\u2028`              | Line Separator              |
+-----------------------+-----------------------------+
| `\u2029`              | Paragraph Separator         |
+-----------------------+-----------------------------+
| `\u202F`              | Narrow No-Break Space       |
+-----------------------+-----------------------------+
| `\u205F`              | Medium Mathematical Space   |
+-----------------------+-----------------------------+
| `\u3000`              | Ideographic Space           |
+-----------------------+-----------------------------+

Example
-------

Using this record as an example:

::

    {
      "id": 1,
      "gender": "    male    ",
      "fname": "    Root    ",
      "lname": "   JOLTIE   ",
      "address": "    67 MARS AVE, MARSCIty, Marsville, Mars"
    }

Applying these directives

::

    trim gender
    ltrim fname
    rtrim lname
    ltrim address

would result in this record:

::

    {
      "id": 1,
      "gender": "male",
      "fname": "Root    ",
      "lname": "   JOLTIE",
      "address": "67 MARS AVE, MARSCIty, Marsville, Mars"
    }
