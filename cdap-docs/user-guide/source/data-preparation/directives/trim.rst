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
| :raw-latex:`\t`       | Character tabulation        |
+-----------------------+-----------------------------+
| :raw-latex:`\n`       | Line Feed (LF)              |
+-----------------------+-----------------------------+
| :raw-latex:`\u0`00B   | Line Tabulation             |
+-----------------------+-----------------------------+
| :raw-latex:`\f`       | Form Feed (FF)              |
+-----------------------+-----------------------------+
| :raw-latex:`\r`       | Carriage Return (CR)        |
+-----------------------+-----------------------------+
| " "                   | Space                       |
+-----------------------+-----------------------------+
| :raw-latex:`\u0`085   | Next line (NEL)             |
+-----------------------+-----------------------------+
| :raw-latex:`\u0`0A0   | No Break Space              |
+-----------------------+-----------------------------+
| :raw-latex:`\u1`680   | OGHAM Space Mark            |
+-----------------------+-----------------------------+
| :raw-latex:`\u1`80E   | Mongolian Vowel Separator   |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`000   | EN Quad                     |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`001   | EM Quad                     |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`002   | EN Space                    |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`003   | EM Space                    |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`004   | Three Per EM space          |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`005   | Four Per EM space           |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`006   | Six Per EM space            |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`007   | Figure Space                |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`008   | Puncatuation Space          |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`009   | Thin Space                  |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`00A   | Hair Space                  |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`028   | Line Separator              |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`029   | Paragraph Separator         |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`02F   | Narrow No-Break Space       |
+-----------------------+-----------------------------+
| :raw-latex:`\u2`05F   | Medium Mathematical Space   |
+-----------------------+-----------------------------+
| :raw-latex:`\u3`000   | Ideographic Space           |
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
