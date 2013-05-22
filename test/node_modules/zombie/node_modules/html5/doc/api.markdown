HTML5
============================================================================

## Synopsis

An example: 

    var HTML5 = require('html5')
    var parser = new HTML5.Parser()
    parser.parse(readableStream)

## Overview

	var HTML5 = require('html5');

Major kinds of object:

	Parser

Parses tokens into a DOM tree

	Tokenizer

Parses textual input into a stream of tokens

	TreeWalker

Walks a DOM tree and emits a stream of tokens

Major functions:

	serialize(sourcetree, target, options)

Will use a TreeWalker to walk a tree, taking the stream of tokens and emitting
'data' events to target, passing a string each time.

## Parser

`Parser` is the entrance to the parsing system. Either call `parse` or
`parseFragment` on it, and it will build a DOM tree.

### Event: 'end'`

`function() { }`

Emitted when done parsing.

### HTML5.Parser.parse(readableStream or string)

Parse the parameter given, if a `readableStream` then asynchronously,
otherwise, immediately.

## serialize(source, target, options)

Serialize the source document tree to the target `outputStream`. If the
target is null or undefined, then return the serialization as a string.

`options` is an optional object to control the output:

### lowercase

Default `true`.

Use lowercase tag names.

### minimize_boolean_attributes

Default `true`.

Given an `input` node with a `readonly` attribute with the value `readonly`,

If `true`, serializes as `<input readonly>`

If `false`, serializes as `<input readonly='readonly'>`

### quote_attr_values

Default `true`

Quotes all attributes, rather than just ones that have spaces in their
values.

### use_best_quote_char

Default `true`

Allows the serializer to choose between single and double quotes as needed
for the simplest serialization.

### use_trailing_solidus

Default `true`

Serializes void elements with a trailing solidus (/), for example, `<link rel='stylesheet' href='...'/>`

### space_before_trailing_solidus

Default `true`

If `true`, serialize void elements with a space before the trailing solidus,
for example `<br />`

If `false`, use the compact XML style, `<br/>` which SGML parsers will not
parse correctly. (the SGML notation is `<br/`, which HTML parsers were
defined to require originally, but never did)

### escape_lt_in_attrs

Escapes `<` characters in attributes
