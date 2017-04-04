.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-parsers-simple-date:

==================
Simple Date Parser
==================

#
Simple Date Parser

PARSE-AS-SIMPLE-DATE is a directive for parsing the date strings.

## Syntax

```
parse-as-simple-date <column> <pattern>
```

## Usage Notes

The PARSE-AS-SIMPLE-DATE directive will parse the given date string, using the given pattern string.
If the column is 'null' or already parsed as date, then applying this directive is a no-op. The column
to be parsed as date should be of type string.

## Examples

The following examples show how date and time patterns are interpreted in the U.S. locale.
The given date and time are 2001-07-04 12:08:56 local time in the U.S. Pacific Time time zone:

+=======================================================================+
| Date and Time Pattern | Date String |
+=======================================================================+
| "yyyy.MM.dd G 'at' HH:mm:ss z" | 2001.07.04 AD at 12:08:56 PDT |
| "EEE, MMM d, ''yy" | Wed, Jul 4, '01 |
| "h:mm a" | 12:08 PM |
| "hh 'o''clock' a, zzzz" | 12 o'clock PM, Pacific Daylight Time |
| "K:mm a, z" | 0:08 PM, PDT |
| "yyyyy.MMMMM.dd GGG hh:mm aaa" | 02001.July.04 AD 12:08 PM |
| "EEE, d MMM yyyy HH:mm:ss Z" | Wed, 4 Jul 2001 12:08:56 -0700 |
| "yyMMddHHmmssZ" | 010704120856-0700 |
| "yyyy-MM-dd'T'HH:mm:ss.SSSZ" | 2001-07-04T12:08:56.235-0700 |
| "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" | 2001-07-04T12:08:56.235-07:00 |
+=======================================================================+

```
parse-as-simple-date entryTime MM/dd/yyyy HH:mm
parse-as-simple-date birthdate yyyy.MM.dd
```
