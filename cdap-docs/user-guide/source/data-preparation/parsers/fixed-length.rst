.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-parsers-fixed-length:

===================
Fixed Length Parser
===================

#
Fixed Length Parser

Parses a column as fixed length record with widths for each field specified.

## Syntax
```
parse-as-fixed-length <column> width[,width]* [padding]
```

## Usage Notes

Fixed width text files are special cases of text files where the format is specified by column widths,
pad character and left/right alignment. Column widths are measured in units of characters.
For example, if you have data in a text file where the first column always has exactly 10 characters,
and the second column has exactly 5, the third has exactly 12 (and so on), this would be categorized as a
fixed width text file.

## Examples

Let's assume a sample record as shown below:

```
{
"body" : "12 10 ABCXYZ"
}
```

applying the PARSE-AS-FIXED-LENGTH directive assumes SPACE as the default padding
character.

```
parse-as-fixed-length body 2,4,5,3
```

would generate the following record

```
{
"body" : "12 10 ABCXYZ",
"body_1" : "12",
"body_2" : " 10",
"body_3" : " ABC",
"body_4" : "XYZ
}
```
