.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===========
Split Email
===========

The SPLIT-EMAIL directive splits an email ID into an account and its
domain.

Syntax
------

::

    split-email <column>

The ``<column>`` is a column containing an email address.

Usage Notes
-----------

The SPLIT-EMAIL directive will parse email address into its constituent
parts: account and domain.

After splitting the email address, the directive will create two new
columns, appending to the original column name:

-  ``column_account``
-  ``column_domain``

If the email address cannot be parsed correctly, the additional columns
will still be generated, but they would be set to ``null`` depending on
the parts that could not be parsed.

Examples
--------

Using this record as an example:

::

    {
      "name": "Root, Joltie",
      "email_address": "root@example.com",
    }

Applying this directive:

::

    split-email email_address

would result in this record:

::

    {
      "name": "Root, Joltie",
      "email_address": "root@example.com",
      "email_address_account": "root",
      "email_address_domain": "example.com"
    }

In case of any errors parsing: when the email address field in the
record is ``null``:

::

    {
      "email": null
    }

this would result in the record:

::

    {
      "email": null,
      "email_account": null,
      "email_domain": null
    }

Using these records as an example, with a variety of email IDs:

::

    [
      { "email": "root@example.org" },
      { "email": "joltie.xxx@gmail.com" },
      { "email": "joltie_xxx@hotmail.com" },
      { "email": "joltie.'@.'root.'@'.@yahoo.com" },
      { "email": "Joltie, Root <joltie.root@hotmail.com>" },
      { "email": "Joltie,Root<joltie.root@hotmail.com>" },
      { "email": "Joltie,Root<joltie.root@hotmail.com" }
    ]

running the directive results in these records:

::

    [
      { "email": "root@example.org", "email_account": "root", "email_domain": "cask.co" },
      { "email": "joltie.xxx@gmail.com", "email_account": "joltie.xxx", "email_domain": "gmail.com" },
      { "email": "joltie_xxx@hotmail.com", "email_account": "joltie_xxx", "email_domain": "hotmail.com" },
      { "email": "joltie.'@.'root.'@'.@yahoo.com", "email_account": "joltie.'@.'root.'@'.", "email_domain": "yahoo.com" },
      { "email": "Joltie, Root <joltie.root@hotmail.com>", "email_account": "joltie.root", "email_domain": "hotmail.com" },
      { "email": "Joltie,Root<joltie.root@hotmail.com>", "email_account": "joltie.root", "email_domain": "hotmail.com" },
      { "email": "Joltie,Root<joltie.root@hotmail.com", "email_account": null, "email_domain": null }
    ]
