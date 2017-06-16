.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-date-transformations:

====================
Date Transformations
====================

- `Diff Date`_
- `Format Date`_


Diff Date
=========
The DIFF-DATE directive calculates the difference between two dates.

Syntax
------
::

  diff-date <column> <column> <destination>

Usage Notes
-----------
The DIFF-DATE directive will take the difference between two Date objects, and put that
difference (in milliseconds) into the ``destination`` column.

This directive can only be used on two columns whose date strings have already
been parsed, either by using the :ref:`PARSE-AS-DATE` or the :ref:`PARSE-AS-SIMPLE-DATE`
directives.

Note that ``date-diff`` can return a negative difference when the first column is an
earlier date than the second column.

If one of the dates in the column is ``null``, then the resulting ``destination`` column
will be null.  If any of the columns contains ``now``, then the column with an actual date is
subtracted from the current time. The ``now`` applies the same date across all the rows.

Examples
--------
Consider this example::

  {
    "create_date" : "02/12/2017",
    "update_date" : "02/14/2017"
  }

Applying this directive::

  diff-date update_date create_date diff_date

will result in this record::

  {
    "create_date" : "02/12/2017",
    "update_date" : "02/14/2017",
    "diff_date" : 17280000
  }


Format Date
===========

FORMAT-DATE directive allows user-defined patterns for date-time formatting.

Syntax
------
::

  format-date <column> <format>


Usage Notes
===========

Date and time formats are specified by date and time pattern strings. Within date and time
pattern strings, unquoted letters from 'A' to 'Z' and from 'a' to 'z' are interpreted as
pattern letters representing the components of a date or time string. Text can be quoted
using single quotes \('\) to avoid interpretation. "''" represents a single quote. All
other characters are not interpreted; they're simply copied into the output string during
formatting or matched against the input string during parsing.

The following pattern letters are defined \(all other characters from 'A' to 'Z' and from
'a' to 'z' are reserved\):

| Letter | Date or Time Component 	| Presentation 	| Examples |
| ------ | ---------------------------- | ------------- | -------- |
| G 	 | Era designator 		| Text 	       	| AD 
| y 	 | Year 			| Year 		| 1996; 96 
| Y 	 | Week year 			| Year 		| 2009; 09 	
| M 	 | Month in year 		| Month July; Jul; 07 
| w 	 | Week in year 		| Number 	| 27 
| W 	 | Week in month 		| Number 	| 2 
| D 	 | Day in year 			| Number 	| 189 
| d 	 | Day in month 		| Number 	| 10 
| F 	 | Day of week in month 	| Number 	| 2 
| E 	 | Day name in week 		| Text 		| Tuesday; Tue 
| a 	 | Am/pm marker 		| Text 		| PM 
| H 	 | Hour in day \(0-23\) 	| Number 	| 0 
| k      | Hour in day \(1-24\) 	| Number 	| 24 
| K	 | Hour in am/pm \(0-11\) 	| Number 	| 0 
| h 	 | Hour in am/pm \(1-12\) 	| Number 	| 12 
| m 	 | Minute in hour 		| Number 	| 30 
| s 	 | Second in minute 		| Number 	| 55 
| S 	 | Millisecond 			| Number 	| 978 
| z 	 | Time zone 			| General time zone | Pacific Standard Time; PST; GMT-08:0 
| Z 	 | Time zone 			| RFC 822 time zone | -0800 
| X 	 | Time zone | ISO 8601 time zone | -08; -0800; -08:00 
