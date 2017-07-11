# Decoder


Description
-----------
Decodes configured fields. Multiple fields can be specified to be decoded using different decoding methods.
Available decoding methods are ``STRING_BASE64``, ``BASE64``, ``BASE32``, ``STRING_BASE32``, and ``HEX``.


Configuration
-------------
**decode:** Specifies the configuration for decode fields; in JSON configuration, 
this is specified as ``<field>:<decoder>[,<field>:<decoder>]*``.

**schema:** Specifies the output schema; the fields that are decoded will have the same field
name but they will be of type ``BYTES`` or ``STRING``.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
