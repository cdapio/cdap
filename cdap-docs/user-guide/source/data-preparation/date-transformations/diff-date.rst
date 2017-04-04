.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-diff-date:

================
Diff Date
================

#


DIFF-DATE is a directive for taking the difference in two dates.

## Syntax

```
diff-date <column> <column> <destination>
```

## Usage Notes

The DIFF-DATE directive will take the difference between two Date objects, and put difference (in milliseconds)
into the destination column.

Note that this directive can only apply on two columns whose date strings have already been parsed, either using the
[PARSE-AS-DATE](docs/directives/parse-as-date.md) directive or the [PARSE-AS-SIMPLE-DATE](docs/directives/parse-as-simple-date.md).

```date-diff``` can return negative difference when first column is an earlier than the second column.

If one of the dates in the column is 'null', then resulting column will be null and if any of the columns
contains "now", then the column with actual date is subtracted from the current time. "now" applies the same
date across all the rows.

## Examples

Let's consider an example
```
{
"create_date" : "02/12/2017",
"update_date" : "02/14/2017"
}
```

Now, applying the following directive

```
diff-date update_date create_date diff_date
```

will result in the record below.

```
{
"create_date" : "02/12/2017",
"update_date" : "02/14/2017",
"diff_date" : 17280000
}
```
