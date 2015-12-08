.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-plugins-transformations-logparser:

==========================
Transformations: LogParser
==========================

.. rubric:: Description

Parses logs from any input source for relevant information such as URI, IP,
browser, device, HTTP status code, and timestamp.

.. rubric:: Use Case

This transform is used when you need to parse log entries. For example, you may
want to read in log files from S3 using S3Batchsource, parse the logs using
LogParserTransform, and then store the IP and URI information in a Cube dataset.


.. rubric:: Properties

**logFormat:** Log format to parse. Currently supports S3, CLF, and Cloudfront formats.

**inputName:** Name of the field in the input schema which encodes the
log information. The given field must be of type String or Bytes.

.. rubric:: Example

::

  {
    "name": "LogParser",
    "properties": {
      "logFormat": "CLF",
      "inputName": "body"
    }
  }

This example searches for an input Schema field named 'body', and then attempts to parse
the Combined Log Format entries found in the field for the URI, IP, browser, device,
HTTP status code, and timestamp. The Transform will emit records with this schema::

  +============================+
  | field name    | type       |
  +============================+
  | uri           | string     |
  | ip            | string     |
  | browser       | string     |
  | device        | string     |
  | httpStatus    | int        |
  | ts            | long       |
  +============================+
