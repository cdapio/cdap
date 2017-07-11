# Compressor


Description
-----------
Compresses configured fields. Multiple fields can be specified to be compressed using different compression algorithms.
Plugin supports SNAPPY, ZIP, and GZIP types of compression of fields.


Configuration
-------------
**compressor:** Specifies the configuration for compressing fields; in JSON configuration, 
this is specified as ``<field>:<compressor>[,<field>:<compressor>]*``.

**schema:** Specifies the output schema; the fields that are compressed will have the same field name 
but they will be of type ``BYTES``.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
