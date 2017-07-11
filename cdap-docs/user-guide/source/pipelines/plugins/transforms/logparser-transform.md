# Log Parser


Description
-----------
Parses logs from any input source for relevant information such as URI, IP,
browser, device, HTTP status code, and timestamp.


Use Case
--------
This transform is used when you need to parse log entries. For example, you may
want to read in log files from S3 using S3Batchsource, parse the logs using
LogParserTransform, and then store the IP and URI information in a Cube dataset.


Properties
----------
**logFormat:** Log format to parse. Currently supports ``S3``, ``CLF``, and ``Cloudfront`` formats.

**inputName:** Name of the field in the input schema which encodes the
log information. The given field must be of type ``String`` or ``Bytes``.

Conditions
----------
If error dataset is configured, then all the erroneous rows, if present in the input, will be committed to the
specified error dataset.
If no error dataset is configured, then pipeline will get completed but with warnings in the logs.

Example
-------
This example searches for an input Schema field named 'body', and then attempts to parse
the Combined Log Format entries found in the field for the URI, IP, browser, device,
HTTP status code, and timestamp:

    {
        "name": "LogParser",
        "type": "transform",
        "properties": {
            "logFormat": "CLF",
            "inputName": "body"
        }
    }

The Transform will emit records with this schema:

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

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
