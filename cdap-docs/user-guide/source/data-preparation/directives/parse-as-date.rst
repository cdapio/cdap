.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=============
Parse as Date
=============

The PARSE-AS-DATE directive is for parsing dates using natural language
processing.

Syntax
------

::

    parse-as-date <column> [<time-zone>]

Usage Notes
-----------

The PARSE-AS-DATE directive will apply standard language recognition and
translation techniques to produce a list of corresponding dates with
optional parse and syntax information.

It recognizes dates described in many ways, including formal dates
(``02/28/1979``), relaxed dates (``oct 1st``), relative dates
(``the day before next thursday``), and even date alternatives
(``next wed or thurs``).

This directive will also search for date components within a larger
block of text, detect their structure, and create dates.

It will create a new column using the syntax ``<column>_1`` containing
the results of parsing.

If ``<time-zone>`` is not provided, UTC is used as the timezone.

Examples
--------

Using this record as an example:

::

    {
      "create_date": "now",
    }

Applying this directive:

::

    parse-as-date create_date US/Eastern

would result in this record (the actual results depending on when this
was run):

::

    {
      "create_date": "now",
      "create_date_1": "Mon May 01 14:13:35 EDT 2017"
    }
