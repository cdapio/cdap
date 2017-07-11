# Decompressor


Description
-----------
Decompresses configured fields. Multiple fields can be specified to be decompressed using
different decompression algorithms. Plugin supports ``SNAPPY``, ``ZIP``, and ``GZIP`` types of
decompression of fields.


Configuration
-------------
**decompressor:** Specifies the configuration for decompressing fields; in JSON configuration, 
this is specified as ``<field>:<decompressor>[,<field>:<decompressor>]*``.

**schema:** Specifies the output schema; the fields that are decompressed will have the same field 
name but they will be of type ``BYTES`` or ``STRING``.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
