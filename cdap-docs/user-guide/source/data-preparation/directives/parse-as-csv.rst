.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

============
Parse as CSV
============

The PARSE-AS-CSV is a directive for parsing an input record as
comma-separated values.

Syntax
------

::

    parse-as-csv <column> <delimiter> [<header=true|false>]

The ``<column>`` specifies the column in the record that should be
parsed as CSV using the specified ``<delimiter>``. If the values in the
first record of the input need to be set as column headers, set
``<header>`` to ``true``; by default, it is set to ``false``.

Examples
--------

Consider a single line from a consumer complaint CSV file. Each line of
the CSV file is added as a record:

::

    {
      "body": "07/29/2013,Consumer Loan,Vehicle Loan,Managing the loan or lease,,,,Wells Fargo & Company,VA,24540,,N/A,Phone,07/30/2013,Closed with explanation,Yes,No,468882"
    }

Applying this directive:

::

    parse-as-csv body ,

would result in this record:

::

    {
      "body": "07/29/2013,Consumer Loan,Vehicle Loan,Managing the loan or lease,,,Wells Fargo & Company,VA,24540,,N/A,Phone,07/30/2013,Closed with explanation,Yes,No,468882",
      "body_1": "07/29/2013",
      "body_2": "Consumer Loan",
      "body_3": "Vehicle Loan",
      "body_4": "Managing the loan or lease",
      "body_5": null,
      "body_6": null,
      "body_7": "Wells Fargo & Company",
      "body_8": "VA",
      "body_9": "24540",
      "body_10": null,
      "body_11": "N/A",
      "body_12": "Phone",
      "body_13": "07/30/2013",
      "body_14": "Closed with explanation",
      "body_15": "Yes",
      "body_16": "No",
      "body_17": "468882"
    }

Using this record, with a header as the first record, as an example:

::

    [
      {
        "body": "Date,Type,Item,Action,Company"
      },
      {
        "body": "07/29/2013,Consumer Loan,Vehicle Loan,Managing the loan or lease,Wells Fargo & Company"
      }
    ]

Applying this directive:

::

    parse-as-csv body , true

would result in this record:

::

    {
      "body": "07/29/2013,Consumer Loan,Vehicle Loan,Managing the loan or lease,Wells Fargo & Company"
      "Date": "07/29/2013",
      "Type": "Consumer Loan",
      "Item": "Vehicle Loan",
      "Action": "Managing the loan or lease",
      "Company": "Wells Fargo & Company"
    }
