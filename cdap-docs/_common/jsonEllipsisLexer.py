# -*- coding: utf-8 -*-
"""
    json-ellipsis-lexer
    ~~~~~~~~~~~~~~~~~~~~

    JSON Lexer that handles an ellipsis in the form of
    three periods on a line of its own.

    :copyright: Copyright 2016-2017 Cask Data.
    :license: Apache License.
    :version: 1.1

"""

from pygments.lexer import include, inherit, bygroups
from pygments.lexers.data import JsonLexer
from pygments.token import Text, Comment

class JsonEllipsisLexer(JsonLexer):
    """
    For coloring JSON examples that have an ellipsis (three periods on a line of its own) to show missing material:
    
    ...
    
    """

    name = 'JSON-E'
    aliases = ['json-ellipsis']

    tokens = {
        'ellipsis': [
            (r'(\s+)(\.{3})(\s*)', bygroups(Text, Comment, Text)),
        ],

        # A JSON object - { attr, attr, ... }
        'objectvalue': [
            include('ellipsis'),
            inherit,
        ],

        # A JSON array - [ value, value, ... }
        'arrayvalue': [
            include('ellipsis'),
            inherit,
        ],

        # A JSON value - either a simple value or a complex value (object or array)
        'value': [
            include('ellipsis'),
            inherit,
        ],
    }
