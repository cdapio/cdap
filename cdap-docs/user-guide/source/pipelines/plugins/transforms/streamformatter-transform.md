# Stream Formatter


Description
-----------
Formats a Structured Record as Stream format. Plugin will convert the Structured Record to Stream format.
It will include a header and body configurations. The body of the Stream event can be either type ``CSV`` or ``JSON``.


Configuration
-------------
**body:** Specifies the input Structured Record fields that should be included in the body of the Stream event.

**header:** Specifies the input Structured Record fields that should be included in the header of the Stream event.

**format:** Specifies the format of the body. Currently supported formats are ``JSON``, ``CSV``, ``TSV``, and ``PSV``.

**schema:** Specifies the output schema; the output schema can have only two fields: one of type ``STRING`` 
and the other of type ``MAP<STRING, STRING>``.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
