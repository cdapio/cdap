.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

==========================
Transformations: LogParser
==========================

.. rubric:: Description: Parses Logs From Any Input Source

Parses logs from any input source for relevant information such as URI, IP, Browser, Device, and
Timestamp.
  
**Log Format:** Log format to parse. Currently supports S3, CLF, and Cloudfront formats.

**Input Name:** Name of the field in the input schema which encodes the
log information. The given field must be of type String or Bytes.