.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

========
Stemming
========

The STEMMING directive applies the Porter stemmer algorithm for English
words. The Porter stemmer has excellent trade-off between speed,
readability, and accuracy. It stems using a set of rules
(transformations) applied in a succession of steps. Generally, it
applies approximately 60 rules in 6 steps.

Syntax
------

::

    stemming <column>

The ``<column>`` contains a bag of words of type string array or type
string list.

Usage Notes
-----------

The STEMMING directive applies the stemmer on a bag of tokenized words.
Applying this directive creates an additional ``<column>_porter``
column. Depending on the type of the object the field is holding, it
will be transformed appropriately.

Example
-------

Using this record, a tokenized bag of words as a string array or list of
strings, as an example:

::

    {
      "word" : { "how are you doing ? do you have apples ?" }
    }

Applying this directive:

::

    stemming word

The result would be this record:

::

    {
      "word": { "how are you doing ? do you have apples ?" },
      "word_porter": { "how", "ar", "you", "do", "do", "you", "have", "appl" }
    }
