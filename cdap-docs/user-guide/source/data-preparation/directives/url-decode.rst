.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

==========
URL Decode
==========

The URL-DECODE directive decodes a string from the
``application/x-www-form-urlencoded`` MIME format to a string.

Syntax
------

::

    url-decode <column>

The ``<column>`` contains an encoded URL to be decoded.

Usage Notes
-----------

The conversion process is the reverse of that used by the
`URL-ENCODE <url-encode.md>`__ directive. It is assumed that all
characters in the encoded string are one of the following: ``a`` through
``z``, ``A`` through ``Z``, ``0`` through ``9``, and ``-``, ``_``,
``.``, and ``*``. The character ``%`` is allowed but is interpreted as
the start of a special escaped sequence.

See https://www.w3.org/TR/html401/interact/forms.html#h-17.13.4.1 for
details.

These rules are applied in the conversion:

-  The alphanumeric characters ``a`` through ``z``, ``A`` through ``Z``,
   and ``0`` through ``9`` remain unchanged
-  The special characters ``.``, ``-``, ``*``, and ``_`` remain
   unchanged
-  The plus sign (``+``) is converted into a space character \` \`
-  A sequence of the form ``%xy`` will be treated as representing a byte
   where ``xy`` is the two-digit hexadecimal representation of the 8-bit
   byte. Then, all substrings that contain one or more of these byte
   sequences consecutively will be replaced by the character(s) whose
   encoding would result in those consecutive bytes. The encoding scheme
   used to decode these characters may be specified, or if unspecified,
   the default encoding of the platform will be used.

**Note:** Uses UTF-8 as the decoding scheme for the string.
