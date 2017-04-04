.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-concepts:

========
Concepts
========


This implementation of wrangler defines the following concepts. Please familiarize yourself with these concepts.

### Record

A Record is a collection of field names and field values.

### Column

A Column is a data value of any supported java type, one for each Record.

### Directive

A Directive is a single data manipulation instruction specified to either transform, filter or pivot a single record into zero or more records. A directive can generate one or more Steps to be executed by the Pipeline.

### Step

A Step is a implementation of a data transformation function operating on a Record or set of records. A step can generate zero or more Records from the application of a function.

### Pipeline

A Pipeline is a collection of Steps to be applied on a Record. Record\(s\) outputed from each Step is passed to the next Step in the pipeline.
