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
| `\u0`00B              | Line Tabulation             |
+-----------------------+-----------------------------+
| `\f`                  | Form Feed (FF)              |
+-----------------------+-----------------------------+
| `\r`                  | Carriage Return (CR)        |
+-----------------------+-----------------------------+
| " "                   | Space                       |
+-----------------------+-----------------------------+
| `\u0`085              | Next line (NEL)             |
+-----------------------+-----------------------------+
| `\u0`0A0              | No Break Space              |
+-----------------------+-----------------------------+
| `\u1`680              | OGHAM Space Mark            |
+-----------------------+-----------------------------+
| `\u1`80E              | Mongolian Vowel Separator   |
+-----------------------+-----------------------------+
| `\u2`000              | EN Quad                     |
+-----------------------+-----------------------------+
| `\u2`001              | EM Quad                     |
+-----------------------+-----------------------------+
| `\u2`002              | EN Space                    |
+-----------------------+-----------------------------+
| `\u2`003              | EM Space                    |
+-----------------------+-----------------------------+
| `\u2`004              | Three Per EM space          |
+-----------------------+-----------------------------+
| `\u2`005              | Four Per EM space           |
+-----------------------+-----------------------------+
| `\u2`006              | Six Per EM space            |
+-----------------------+-----------------------------+
| `\u2`007              | Figure Space                |
+-----------------------+-----------------------------+
| `\u2`008              | Puncatuation Space          |
+-----------------------+-----------------------------+
| `\u2`009              | Thin Space                  |
+-----------------------+-----------------------------+
| `\u2`00A              | Hair Space                  |
+-----------------------+-----------------------------+
| `\u2`028              | Line Separator              |
+-----------------------+-----------------------------+
| `\u2`029              | Paragraph Separator         |
+-----------------------+-----------------------------+
| `\u2`02F              | Narrow No-Break Space       |
+-----------------------+-----------------------------+
| `\u2`05F              | Medium Mathematical Space   |
+-----------------------+-----------------------------+
| `\u3`000              | Ideographic Space           |
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
