.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=========
JSON Path
=========

The JSON-PATH directive uses a DSL for reading JSON records.

Syntax
------

::

    json-path <source-column> <destination-column> <expression>

-  ``<source-column>`` specifies the column in the record that should be
   considered as the "root member object" or "$"
-  ``<destination-column>`` is the name of the output column in the
   record where the results of the expression will be stored
-  ``<expression>`` is a JSON path expression; see *Usage Notes* below
   for details

Usage Notes
-----------

An expression always refers to a JSON structure in the same way that an
XPath expression is used in combination with an XML document. The "root
member object" is always referred to as ``$`` regardless if it is an
object or an array.

Notation
~~~~~~~~

Expressions can use either the "dot–notation":

::

    $.name.first

or the "bracket–notation":

::

    $['name']['first']

Operators
~~~~~~~~~

+-------------------------------+---------------------------------------------------------------+
| Operator                      | Description                                                   |
+===============================+===============================================================+
| ``$``                         | The root element to query; this starts all path expressions   |
+-------------------------------+---------------------------------------------------------------+
| ``@``                         | The current node being processed by a filter predicate        |
+-------------------------------+---------------------------------------------------------------+
| ``*``                         | Wildcard; available anywhere a name or numeric are required   |
+-------------------------------+---------------------------------------------------------------+
| ``..``                        | Deep scan; available anywhere a name is required              |
+-------------------------------+---------------------------------------------------------------+
| ``.<name>``                   | Dot-notated child                                             |
+-------------------------------+---------------------------------------------------------------+
| ``['<name>' (, '<name>')]``   | Bracket-notated child or children                             |
+-------------------------------+---------------------------------------------------------------+
| ``[<number> (, <number>)]``   | Array index or indexes                                        |
+-------------------------------+---------------------------------------------------------------+
| ``[start:end]``               | Array slice operator                                          |
+-------------------------------+---------------------------------------------------------------+
| ``[?(<expression>)]``         | Filter expression; must evaluate to a boolean value           |
+-------------------------------+---------------------------------------------------------------+

Functions
~~~~~~~~~

Functions can be invoked at the tail end of a path: the input to a
function is the output of the path expression. The function output is
dictated by the function itself.

+----------------+-------------------------------------------------------+-----------+
| Function       | Returns                                               | Output    |
+================+=======================================================+===========+
| ``min()``      | The min value of an array of numbers                  | Double    |
+----------------+-------------------------------------------------------+-----------+
| ``max()``      | The max value of an array of numbers                  | Double    |
+----------------+-------------------------------------------------------+-----------+
| ``avg()``      | The average value of an array of numbers              | Double    |
+----------------+-------------------------------------------------------+-----------+
| ``stddev()``   | The standard deviation value of an array of numbers   | Double    |
+----------------+-------------------------------------------------------+-----------+
| ``length()``   | The length of an array                                | Integer   |
+----------------+-------------------------------------------------------+-----------+

Filter Operators
~~~~~~~~~~~~~~~~

Filters are logical expressions used to filter arrays. A typical filter
would be:

::

    [?(@.age>18)]

where ``@`` represents the current item being processed.

-  More complex filters can be created with the logical operators ``&&``
   and ``||``

-  String literals must be enclosed by either single or double quotes,
   such as in ``[?(@.color=='blue')]`` or ``[?(@.color=="blue")]``

+-------------------+---------------------------------------------------------------------------------+
| Filter Operator   | Description                                                                     |
+===================+=================================================================================+
| ``==``            | Left is equal in type and value to right (note ``1`` is not equal to ``'1'``)   |
+-------------------+---------------------------------------------------------------------------------+
| ``!=``            | Left is not equal to right                                                      |
+-------------------+---------------------------------------------------------------------------------+
| ``<``             | Left is less than right                                                         |
+-------------------+---------------------------------------------------------------------------------+
| ``<=``            | Left is less than or equal to right                                             |
+-------------------+---------------------------------------------------------------------------------+
| ``>``             | Left is greater than right                                                      |
+-------------------+---------------------------------------------------------------------------------+
| ``>=``            | Left is greater than or equal to right                                          |
+-------------------+---------------------------------------------------------------------------------+
| ``=~``            | Left matches regular expression ``[?(@.name=~/foo.*?/i)]``                      |
+-------------------+---------------------------------------------------------------------------------+
| ``in``            | Left exists in right ``[?(@.size in ['S', 'M'])]``                              |
+-------------------+---------------------------------------------------------------------------------+
| ``nin``           | Left does not exist in right                                                    |
+-------------------+---------------------------------------------------------------------------------+
| ``size``          | Size of left (array or string) matches right                                    |
+-------------------+---------------------------------------------------------------------------------+
| ``empty``         | Left (array or string) is empty                                                 |
+-------------------+---------------------------------------------------------------------------------+

Examples
--------

Using this record as an example:

.. code:: json

    {
        "store": {
            "book": [
                {
                    "category": "reference",
                    "author": "Nigel Rees",
                    "title": "Sayings of the Century",
                    "price": 8.95
                },
                {
                    "category": "fiction",
                    "author": "Evelyn Waugh",
                    "title": "Sword of Honour",
                    "price": 12.99
                },
                {
                    "category": "fiction",
                    "author": "Herman Melville",
                    "title": "Moby Dick",
                    "isbn": "0-553-21311-3",
                    "price": 8.99
                },
                {
                    "category": "fiction",
                    "author": "J. R. R. Tolkien",
                    "title": "The Lord of the Rings",
                    "isbn": "0-395-19395-8",
                    "price": 22.99
                }
            ],
            "bicycle": {
                "color": "red",
                "price": 19.95
            }
        },
        "expensive": 10
    }

+------------------------------------------------+---------------------------+
| JSON Path (click link to test)                 | Result                    |
+================================================+===========================+
| `$.store.book[\*].author <http://jsonpath.hero | The authors of all books  |
| kuapp.com/?path=$.store.book%5B*%5D.author>`__ |                           |
+------------------------------------------------+---------------------------+
| `$..author <http://jsonpath.herokuapp.com/?pat | All authors               |
| h=$..author>`__                                |                           |
+------------------------------------------------+---------------------------+
| `$.store.\* <http://jsonpath.herokuapp.com/?pa | All things, both books    |
| th=$.store.*>`__                               | and bicycles              |
+------------------------------------------------+---------------------------+
| `$.store..price <http://jsonpath.herokuapp.com | The price of everything   |
| /?path=$.store..price>`__                      |                           |
+------------------------------------------------+---------------------------+
| `$..book[2] <http://jsonpath.herokuapp.com/?pa | The third book            |
| th=$..book%5B2%5D>`__                          |                           |
+------------------------------------------------+---------------------------+
| `$..book[0,1] <http://jsonpath.herokuapp.com/? | The first two books       |
| path=$..book%5B0,1%5D>`__                      |                           |
+------------------------------------------------+---------------------------+
| `$..book[:2] <http://jsonpath.herokuapp.com/?p | All books from index 0    |
| ath=$..book%5B:2%5D>`__                        | (inclusive) until index 2 |
|                                                | (exclusive)               |
+------------------------------------------------+---------------------------+
| `$..book[1:2] <http://jsonpath.herokuapp.com/? | All books from index 1    |
| path=$..book%5B1:2%5D>`__                      | (inclusive) until index 2 |
|                                                | (exclusive)               |
+------------------------------------------------+---------------------------+
| `$..book[-2:] <http://jsonpath.herokuapp.com/? | Last two books            |
| path=$..book%5B-2:%5D>`__                      |                           |
+------------------------------------------------+---------------------------+
| `$..book[2:] <http://jsonpath.herokuapp.com/?p | Book number two from tail |
| ath=$..book%5B2:%5D>`__                        |                           |
+------------------------------------------------+---------------------------+
| `$..book[?(@.isbn)] <http://jsonpath.herokuapp | All books with an ISBN    |
| .com/?path=$..book%5B?(@.isbn)%5D>`__          | number                    |
+------------------------------------------------+---------------------------+
| `$..book[?(@.isbn)] <http://jsonpath.herokuapp | All books with an ISBN    |
| .com/?path=$..book%5B?(@.isbn)%5D>`__          | number                    |
+------------------------------------------------+---------------------------+
| `$.store.book[?(@.price<10)] <http://jsonpath. | All books in store        |
| herokuapp.com/?path=$.store.book%5B?(@.price%3 | cheaper than 10           |
| C10)%5D>`__                                    |                           |
+------------------------------------------------+---------------------------+
| `:math:`..book[?(@.price<=`\ ['expensive'])] < | All books in store that   |
| http://jsonpath.herokuapp.com/?path=$..book%5B | are not "expensive"       |
| ?(@.price%3C=$%5B'expensive'%5D)%5D>`__        |                           |
+------------------------------------------------+---------------------------+
| `$..book[?(@.author=~/.\*REES/i)] <http://json | All books matching a      |
| path.herokuapp.com/?path=$..book%5B?(@.author= | regex (ignore case)       |
| ~/.*REES/i)%5D>`__                             |                           |
+------------------------------------------------+---------------------------+
| `$..\* <http://jsonpath.herokuapp.com/?path=$. | All books                 |
| .*>`__                                         |                           |
+------------------------------------------------+---------------------------+
| `$..book.length() <http://jsonpath.herokuapp.c | The number of books       |
| om/?path=$..book.length()>`__                  |                           |
+------------------------------------------------+---------------------------+
