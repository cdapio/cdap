# -*- coding: utf-8 -*-
"""
    json-ellipsis-lexer
    ~~~~~~~~~~~~~~~~~~~~

    JSON Lexer thta handles an ellipsis in the form of
    three periods on a line of its own.

    :copyright: Copyright 2016 Cask Data.
    :license: Apache License.
    
"""

from pygments.lexer import include, inherit, bygroups
from pygments.lexers.data import JsonLexer
from pygments.token import Text, Comment

class JsonEllipsisLexer(JsonLexer):
    """
    For JSON examples with ellipses (three periods on a line of its own):
    
    ...
    
    """

    name = 'JSON-E'
    aliases = ['json-ellipsis']

    tokens = {
        'ellipsis': [
            (r'(\s*)(\.{3})(\s*)', bygroups(Text, Comment, Text)),
        ],

        # a json value - either a simple value or a complex value (object or array)
        'value': [
            inherit,
            include('ellipsis'),
        ],
    }
