.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-parsers-date:

===========
Date Parser
===========

PARSE-AS-DATE is a directive for parsing date using natural language processing.

Syntax
======
::

  parse-as-date <column> [<timezone>]

Usage Notes
===========

The PARSE-AS-DATE directive will apply standard language recognition and translation techniques to produce a list of
corresponding dates with optional parse and syntax information.

It recognizes dates described in many ways, including formal dates (02/28/1979), relaxed dates (oct 1st),
relative dates (the day before next thursday), and even date alternatives (next wed or thurs).

This directive will also search for date components within a larger block of text, detect their structure, and
create dates.

Examples
========
::

  parse-as-date now
  parse-as-date June 15th 2017

