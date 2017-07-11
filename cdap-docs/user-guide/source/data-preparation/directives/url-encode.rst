.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

==========
URL Encode
==========

The URL-ENCODE directive encodes a string to the
``application/x-www-form-urlencoded`` MIME format.

Syntax
------

::

    url-encode <column>

The ``<column>`` contains a URL to be encoded.

Usage Notes
-----------

See https://www.w3.org/TR/html401/interact/forms.html#h-17.13.4.1 for
details of these rules.

When encoding a string, these rules apply:

-  The alphanumeric characters ``a`` through ``z``, ``A`` through ``Z``,
   and ``0`` through ``9`` remain unchanged
-  The special characters ``.``, ``-``, ``*``, and ``_`` remain
   unchanged
-  The space character ``` is converted into a plus sign (``\ +\`)
-  All other characters are considered unsafe and are first converted
   into one or more bytes using an encoding scheme. Each byte is then
   represented by a 3-character string ``%xy``, where ``xy`` is the
   two-digit hexadecimal representation of the byte. The recommended
   encoding scheme to use is UTF-8. However, for compatibility reasons,
   if an encoding is not specified, then the default encoding of the
   platform is used.

**Note:** Uses UTF-8 as the encoding scheme for the string.

See also the `URL-DECODE <url-decode.md>`__ directive.
