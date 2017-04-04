# CSV Formatter


Description
-----------
Formats a Structured Record as a CSV Record. Supported CSV Record formats are ``DELIMITED``,
``EXCEL``, ``MYSQL``, ``RFC4180``, and ``TDF``. When the format is ``DELIMITED``, one can specify different
delimiters that a CSV Record should use for separating fields.


Configuration
-------------
**format:** Specifies the format of the CSV Record to be generated.

**delimiter:** Specifies the delimiter to be used to generate a CSV Record; 
this option is available when the format is specified as ``DELIMITED``.

**schema:** Specifies the output schema. Output schema should only have fields of type String.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
