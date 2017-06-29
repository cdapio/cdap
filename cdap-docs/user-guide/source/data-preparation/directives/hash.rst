.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

====
Hash
====

The HASH directive generates a message digest.

Syntax
------

::

    hash <column> <algorithm> [<encode>]

The ``<column>`` is the name of the column to which the hashing
``<algorithm>`` is applied.

If ``<encode>`` is set to ``true``, the hashed digest is encoded as
``hex`` with left-padding zeroes. By default, ``<encode>`` is set to
``true``. To disable ``hex`` encoding, set ``<encode>`` to false.

Usage Notes
-----------

The HASH directive, when applied on a ``<column>``, will replace the
content of the column with the message hash. No new columns are created
with the application of this directive.

These algorithms are supported by the HASH directive:

-  BLAKE2B-160
-  BLAKE2B-256
-  BLAKE2B-384
-  BLAKE2B-512
-  GOST3411
-  GOST3411-2012-256
-  GOST3411-2012-512
-  KECCAK-224
-  KECCAK-256
-  KECCAK-288
-  KECCAK-384
-  KECCAK-512
-  MD2
-  MD4
-  MD5
-  RIPEMD128
-  RIPEMD160
-  RIPEMD256
-  RIPEMD320
-  SHA
-  SHA-1
-  SHA-224
-  SHA-256
-  SHA-384
-  SHA-512
-  SHA-512/224
-  SHA-512/256
-  SHA3-224
-  SHA3-256
-  SHA3-384
-  SHA3-512
-  Skein-1024-1024
-  Skein-1024-384
-  Skein-1024-512
-  Skein-256-128
-  Skein-256-160
-  Skein-256-224
-  Skein-256-256
-  Skein-512-128
-  Skein-512-160
-  Skein-512-224
-  Skein-512-256
-  Skein-512-384
-  Skein-512-512
-  SM3
-  Tiger
-  WHIRLPOOL

Example
-------

Using this record as an example:

::

    {
      "message": "secret message"
    }

Applying this directive:

::

    hash message SHA3-384

would generate a message digest and replace the column with it:

::

    {
      "message": "9cc25835d1ef78b4cd8b36a0c4ad636a6094fbb944b1d880f21c7129a645e819d3be987e8ae2f0f8d6cbebb8452419ef"
    }

The column ``message`` is replaced with the digest created using the
SHA3-384 algorithm. The type of column is a string.

**Note:** By default, encoding is on. When ``<encode>`` is set to
``false``, the output column type is a byte array.
