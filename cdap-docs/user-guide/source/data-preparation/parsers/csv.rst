.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-parsers-csv:

==========
CSV Parser
==========


PARSE-AS-CSV is a directive for parsing an input record as comma separated value.

Syntax
======
::

  parse-as-csv <column-name> <delimiter> <skip-on-error>

`column-name` specifies the name of the column in the record that should be parsed as CSV using the `delimiter` specified. Often times there are empty lines in file\(s\) that need to be skipped, set `skip-on-error` to true, by default it's set to false.

Examples
========

Let's look at an example. Let's a consider a single line from the consumer complaint CSV file. Each line of the CSV file is added to a record::
  
  {
    "body" : "07/29/2013,Consumer Loan,Vehicle loan,Managing the loan or lease,,,,Wells Fargo & Company,VA,24540,,N/A,Phone,07/30/2013,Closed with explanation,Yes,No,468882"
  }

Now, let's apply the directive to see the results::


  parse-as-csv body , true

applying the above directive would result in record as show below::

  {
    "body" : "07/29/2013,Consumer Loan,Vehicle loan,Managing the loan or lease,,,,Wells Fargo & Company,VA,24540,,N/A,Phone,07/30/2013,Closed with explanation,Yes,No,468882",
    "body_1" : "07/29/2013",
    "body_2" : "Consumer Loan,Vehicle loan",
    "body_3" : "Managing the loan or lease",
    "body_4" : null,
    "body_5" : null,
    "body_6" : null,
    "body_7" : "Wells Fargo & Company",
    "body_8" : "VA",
    "body_9" : "24540",
    "body_10" : null,
    "body_11" : "N/A",
    "body_12" : "Phone",
    "body_13" : "07/30/2013",
    "body_14" : "Closed with explanation",
    "body_15" : "Yes",
    "body_16" : "No",
    "body_17" : "468882"
  }


