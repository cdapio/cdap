# Encoder


Description
-----------
Encodes configured fields. Multiple fields can be specified to be encoded using different encoding methods.
Available encoding methods are ``STRING_BASE64``, ``BASE64``, ``BASE32``, ``STRING_BASE32``, and ``HEX``.


Configuration
-------------
**encode:** Specifies the configuration for encode fields; in JSON configuration, 
this is specified as ``<field>:<encoder>[,<field>:<encoder>]*``.

**schema:** Specifies the output schema; the fields that are encoded will have the same field name 
but they will be of type ``BYTES`` or ``STRING``.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
