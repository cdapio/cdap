.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-parsers-json-path:

=================
JSON Path Parsers
=================

#
Json Path

JSON-PATH directive uses a DSL for reading json records.

## Syntax

::

  json-path <source-column> <destination-column> <expression>

- ``source-column``: specifies the name of the column in the record that should be
  considered as "root member object" or "$".
- ``destination-column`` is the name of the output column in the record where the results
  of expression will be stored.
- ``expression`` is a JSON path expression; see below for more details.

## Usage Notes

The expressions always refer to a Json structure in the same way as XPath expression are
used in combination with an XML document.The "root member object" is always referred to
as`$`regardless if it is an object or array.

Expressions can use the dot–notation::

  $.name.first

or the bracket–notation::

  $['name']['first']


Operators
=========
::

  | Operator | Description |
  | :--- | :--- |
  | `$` | The root element to query. This starts all path expressions. |
  | `@` | The current node being processed by a filter predicate. |
  | `*` | Wildcard. Available anywhere a name or numeric are required. |
  | `..` | Deep scan. Available anywhere a name is required. |
  | `.<name>` | Dot-notated child |
  | `['<name>' (, '<name>')]` | Bracket-notated child or children |
  | `[<number> (, <number>)]` | Array index or indexes |
  | `[start:end]` | Array slice operator |
  | `[?(<expression>)]` | Filter expression. Expression must evaluate to a boolean value. |

Functions
=========

Functions can be invoked at the tail end of a path; the input to a function is the output
of the path expression. The function output is dictated by the function itself.

::

  | Function | Description | Output |
  | :--- | :--- | :--- |
  | min\(\) | Provides the min value of an array of numbers | Double |
  | max\(\) | Provides the max value of an array of numbers | Double |
  | avg\(\) | Provides the average value of an array of numbers | Double |
  | stddev\(\) | Provides the standard deviation value of an array of numbers | Double |
  | length\(\) | Provides the length of an array | Integer |

Filter Operators
================

Filters are logical expressions used to filter arrays. A typical filter would be`[?(@.age
> 18)]`where`@`represents the current item being processed. More complex filters can be
created with logical operators`&&`and`||`. String literals must be enclosed by single or
double quotes \(`[?(@.color == 'blue')]`or`[?(@.color == "blue")]`\).

::

  | Operator | Description |
  | :--- | :--- |
  | == | left is equal to right \(note that 1 is not equal to '1'\) |
  | != | left is not equal to right |
  | &lt; | left is less than right |
  | &lt;= | left is less or equal to right |
  | &gt; | left is greater than right |
  | &gt;= | left is greater than or equal to right |
  | =~ | left matches regular expression \[?\(@.name =~ /foo.\*?/i\)\] |
  | in | left exists in right \[?\(@.size in \['S', 'M'\]\)\] |
  | nin | left does not exists in right |
  | size | size of left \(array or string\) should match right |
  | empty | left \(array or string\) should be empty |

Example
=======

Given the JSON::

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

::

  | JsonPath \(click link to try\) | Result |
  | :--- | :--- |
  | [$.store.book\[\*\].author](http://jsonpath.herokuapp.com/?path=$.store.book[*].author) | The authors of all books |
  | [$..author](http://jsonpath.herokuapp.com/?path=$..author) | All authors |
  | [$.store.\*](http://jsonpath.herokuapp.com/?path=$.store.*) | All things, both books and bicycles |
  | [$.store..price](http://jsonpath.herokuapp.com/?path=$.store..price) | The price of everything |
  | [$..book\[2\]](http://jsonpath.herokuapp.com/?path=$..book[2]) | The third book |
  | [$..book\[0,1\]](http://jsonpath.herokuapp.com/?path=$..book[0,1]) | The first two books |
  | [$..book\[:2\]](http://jsonpath.herokuapp.com/?path=$..book[:2]) | All books from index 0 \(inclusive\) until index 2 \(exclusive\) |
  | [$..book\[1:2\]](http://jsonpath.herokuapp.com/?path=$..book[1:2]) | All books from index 1 \(inclusive\) until index 2 \(exclusive\) |
  | [$..book\[-2:\]](http://jsonpath.herokuapp.com/?path=$..book[-2:]) | Last two books |
  | [$..book\[2:\]](http://jsonpath.herokuapp.com/?path=$..book[2:]) | Book number two from tail |
  | [$..book\[?\(@.isbn\)\]](http://jsonpath.herokuapp.com/?path=$..book[?%28@.isbn%29]) | All books with an ISBN number |
  | [$.store.book\[?\(@.price &lt; 10\)\]](http://jsonpath.herokuapp.com/?path=$.store.book[?%28@.price < 10%29]) | All books in store cheaper than 10 |
  | \[$..book\[?\(@.price &lt;= $\['expensive'\]\)\]\]\([http://jsonpath.herokuapp.com/?path=$..book\[?\(@.price](http://jsonpath.herokuapp.com/?path=$..book[?%28@.price) &lt;= $\['expensive'\]%29\]\) | All books in store that are not "expensive" |
  | [$..book\[?\(@.author =~ /.\*REES/i\)\]](http://jsonpath.herokuapp.com/?path=$..book[?%28@.author =~ /.*REES/i%29]) | All books matching regex \(ignore case\) |
  | [$..\*](http://jsonpath.herokuapp.com/?path=$..*) | Give me every thing |
  | [$..book.length\(\)](http://jsonpath.herokuapp.com/?path=$..book.length%28%29) | The number of books |
