
################################################################################
####  NOTE:  THIS IS STILL IN DEVELOPMENT:                                  ####
####                                                                        ####
####    - No encoder                                                        ####
####    - Needs more tests!                                                 ####
####                                                                        ####
################################################################################

'''
RSON -- readable serial object notation

RSON is a superset of JSON with relaxed syntax for human readability.

Simple usage example:
            import rson
            obj = rson.loads(source)

Additional documentation available at:

http://code.google.com/p/rson/
'''

__version__ = '0.08'

__author__ = 'Patrick Maupin <pmaupin@gmail.com>'

__copyright__ = '''
Copyright (c) 2010, Patrick Maupin.  All rights reserved.

 Permission is hereby granted, free of charge, to any person
 obtaining a copy of this software and associated documentation
 files (the "Software"), to deal in the Software without
 restriction, including without limitation the rights to use,
 copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the
 Software is furnished to do so, subject to the following
 conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 OTHER DEALINGS IN THE SOFTWARE.
 '''

import bisect
import re
import sys

class RSONDecodeError(ValueError):
    pass

class Tokenizer(list):
    ''' The RSON tokenizer uses re.split() to rip the source string
        apart into smaller strings which may or may not be true standalone
        tokens.  Sufficient information is maintained to put Humpty-Dumpty
        back together when necessary.

        The tokens are kept in a reversed list.  This makes retrieving
        the next token a low-cost pop() operation from the end of the list,
        and facilitates arbitrary lookahead operations.

        Each token is a tuple, containing the following elements:
           [0] Negative character offset of token within the source string.
               A negative offset is used so that the tokens are sorted
               properly for bisect() for special operations.
           [1] single-character string usually containing a character
               which represents the token type (often the entire token).
           [2] string containing entire token
           [3] string (possibly null) containing whitespace after token
           [4] Indentation value of line containing token
                            (\n followed by 0 or more spaces)
           [5] Line number of token
           [6] Tokenizer object that the token belongs to (for error reporting)
    '''

    # Like Python, indentation is special.  I originally planned on making
    # the space character the only valid indenter, but that got messy
    # when combined with the desire to be 100% JSON compatible, so, like
    # JSON, you can indent with any old whitespace, but if you mix and
    # match, you might be in trouble (like with Python).
    #
    # An indentation is always the preceding EOL plus optional spaces,
    # so we create a dummy EOL for the very start of the string.
    # Could also have an embedded comment
    indentation = r'\n[ \t\v\f]*(?:#.*)?'

    # RSON syntax delimiters are tokenized separately from everything else.
    delimiterset = set(' { } [ ] : = , '.split())

    re_delimiterset = ''.join(delimiterset).replace(']', r'\]')

    # Create a RE pattern for the delimiters
    delimiter_pattern = '[%s]' % re_delimiterset

    # A regular quoted string must terminate before the end of the line,
    # and \ can be used as the internal escape character.
    quoted_string = r'"(?:[^"\n\\]|\\.)*(?:"|(?=\n))'

    # A triple-quoted string can contain any characters.  The only escape
    # processing that is done on them is to allow a backslash in front of
    # another set of triple quotes.  We only look for the start of one of
    # these suckers in the first pass, and then go find the real string
    # later.  This keeps us from getting our knickers in a twist on
    # regular strings vs triple-quoted vs comments, etc.

    triple_quoted_string = '"""'

    # Any non-whitespace, non-delimiter, group of characters is in the "other"
    # category.  This group can have embedded whitespace, but ends on a
    # non-whitespace character.

    other = r'[\S](?:[^%s\n]*[^%s\s])*' % (re_delimiterset, re_delimiterset)

    pattern = '(%s)' % '|'.join([
      delimiter_pattern,
      triple_quoted_string,
      quoted_string,
      other,
      indentation,
    ])

    splitter = re.compile(pattern).split

    @classmethod
    def factory(cls, len=len, iter=iter, unicode=unicode, isinstance=isinstance):
        splitter = cls.splitter
        delimiterset = set(cls.delimiterset) | set('"')

        def newstring(source, client):
            self = cls()
            self.client = client

            # Deal with 8 bit bytes for now
            if isinstance(source, unicode):
                source = source.encode('utf-8')

            # Convert MS-DOS or Mac line endings to the one true way
            source = source.replace('\r\n', '\n').replace('\r', '\n')
            sourcelist = splitter(source)

            # Get the indentation at the start of the file
            indentation = '\n' + sourcelist[0]
            linenum = 1
            linestart = offset = 0

            # Set up to iterate over the source and add to the destination list
            sourceiter = iter(sourcelist)
            next = sourceiter.next
            offset -= len(next())

            # Strip comment from first line
            if len(sourcelist) > 1 and sourcelist[1].startswith('#'):
                i = 1
                while len(sourcelist) > i and not sourcelist[i].startswith('\n'):
                    i += 1
                    offset -= len(next())


            # Preallocate the list
            self.append(None)
            self *= len(sourcelist) / 2 + 1
            index = 0

            # Create all the tokens
            for token in sourceiter:
                whitespace = next()
                t0 = token[0]
                if t0 not in delimiterset:
                    if t0 == '\n':
                        linenum += 1
                        indentation = token
                        offset -= len(token)
                        linestart = offset
                        continue
                    else:
                        t0 = 'X'
                self[index] = (offset, t0, token, whitespace, indentation, linenum, self)
                index += 1
                offset -= len(token) + len(whitespace)

            # Add a sentinel
            self[index] = (offset, '@', '@', '', '', linenum + 1, self)
            self[index+1:] = []

            # Put everything we need in the actual object instantiation
            self.reverse()
            self.source = source
            self.next = self.pop
            self.push = self.append
            return self
        return newstring

    def peek(self):
        return self[-1]

    def lookahead(self, index=0):
        return self[-1 - index]

    @staticmethod
    def sourceloc(token):
        ''' Return the source location for a given token
        '''
        lineno = token[5]
        colno = offset = -token[0] + 1
        if lineno != 1:
            colno -= token[-1].source.rfind('\n', 0, offset) + 1
        return offset, lineno, colno

    @classmethod
    def error(cls, s, token):
        ''' error performs generic error reporting for tokens
        '''
        offset, lineno, colno = cls.sourceloc(token)

        if token[1] == '@':
            loc = 'at end of string'
        else:
            text = token[2]
            loc = 'line %s, column %s, text %s' % (lineno, colno, repr(text[:20]))

        err = RSONDecodeError('%s: %s' % (s, loc))
        err.pos = offset
        err.lineno = lineno
        err.colno = colno
        raise err


def make_hashable(what):
    try:
        hash(what)
        return what
    except TypeError:
        if isinstance(what, dict):
            return tuple(sorted(make_hashable(x) for x in what.iteritems()))
        return tuple(make_hashable(x) for x in what)

class BaseObjects(object):

    # These hooks allow compatibility with simplejson
    object_hook = None
    object_pairs_hook = None
    array_hook = None

    # Stock object constructor does not cope with no keys
    disallow_missing_object_keys = True

    # Stock object constructor copes with multiple keys just fine
    disallow_multiple_object_keys = False

    # Default JSON requires string keys
    disallow_nonstring_keys = True

    class default_array(list):
        def __new__(self, startlist, token):
            return list(startlist)

    class default_object(dict):
        ''' By default, RSON objects are dictionaries that
            allow attribute access to their existing contents.
        '''
        
        def __getattr__(self, key):
            return self[key]
        def __setattr__(self, key, value):
            self[key]=value

        def append(self, itemlist):
            mydict = self
            value = itemlist.pop()
            itemlist = [make_hashable(x) for x in itemlist]
            lastkey = itemlist.pop()

            if itemlist:
                itemlist.reverse()
                while itemlist:
                    key = itemlist.pop()
                    subdict = mydict.get(key)
                    if not isinstance(subdict, dict):
                        subdict = mydict[key] = type(self)()
                    mydict = subdict
            if isinstance(value, dict):
                oldvalue = mydict.get(lastkey)
                if isinstance(oldvalue, dict):
                    oldvalue.update(value)
                    return
            mydict[lastkey] = value

        def get_result(self, token):
            return self

    def object_type_factory(self, dict=dict, tuple=tuple):
        ''' This function returns constructors for RSON objects and arrays.
            It handles simplejson compatible hooks as well.
        '''
        object_hook = self.object_hook
        object_pairs_hook = self.object_pairs_hook

        if object_pairs_hook is not None:
            class build_object(list):
                def get_result(self, token):
                    return object_pairs_hook([tuple(x) for x in self])
            self.disallow_multiple_object_keys = True
            self.disallow_nonstring_keys = True
        elif object_hook is not None:
            mydict = dict
            class build_object(list):
                def get_result(self, token):
                    return object_hook(mydict(self))
            self.disallow_multiple_object_keys = True
            self.disallow_nonstring_keys = True
        else:
            build_object = self.default_object

        build_array = self.array_hook or self.default_array
        return build_object, build_array


class Dispatcher(object):
    ''' Assumes that this is mixed-in to a class with an
        appropriate parser_factory() method.
    '''

    @classmethod
    def dispatcher_factory(cls, hasattr=hasattr, tuple=tuple, sorted=sorted):

        self = cls()
        parser_factory = self.parser_factory
        parsercache = {}
        cached = parsercache.get
        default_loads = parser_factory()

        def loads(s, **kw):
            if not kw:
                return default_loads(s)

            key = tuple(sorted(kw.iteritems()))
            func = cached(key)
            if func is None:
                # Begin some real ugliness here -- just modify our instance to
                # have the correct user variables for the initialization functions.
                # Seems to speed up simplejson testcases a bit.
                self.__dict__ = dict((x,y) for (x,y) in key if y is not None)
                func = parsercache[key] = parser_factory()

            return func(s)

        return loads


class QuotedToken(object):
    ''' Subclass or replace this if you don't like quoted string handling
    '''

    parse_quoted_str = staticmethod(
          lambda token, s, unicode=unicode: unicode(s, 'utf-8'))
    parse_encoded_chr = unichr
    parse_join_str = u''.join
    cachestrings = False

    quoted_splitter = re.compile(r'(\\u[0-9a-fA-F]{4}|\\.|")').split
    quoted_mapper = { '\\\\' : u'\\',
               r'\"' : u'"',
               r'\/' : u'/',
               r'\b' : u'\b',
               r'\f' : u'\f',
               r'\n' : u'\n',
               r'\r' : u'\r',
               r'\t' : u'\t'}.get

    def quoted_parse_factory(self, int=int, iter=iter, len=len):
        quoted_splitter = self.quoted_splitter
        quoted_mapper = self.quoted_mapper
        parse_quoted_str = self.parse_quoted_str
        parse_encoded_chr = self.parse_encoded_chr
        parse_join_str = self.parse_join_str
        cachestrings = self.cachestrings
        triplequoted = self.triplequoted

        allow_double = sys.maxunicode > 65535

        def badstring(token, special):
            if token[2] != '"""' or triplequoted is None:
                token[-1].error('Invalid character in quoted string: %s' % repr(special), token)
            result = parse_quoted_str(token, triplequoted(token))
            if cachestrings:
                result = token[-1].stringcache(result, result)
            return result

        def parse(token, next):
            s = token[2]
            if len(s) < 2 or not (s[0] == s[-1] == '"'):
                token[-1].error('No end quote on string', token)
            s = quoted_splitter(s[1:-1])
            result = parse_quoted_str(token, s[0])
            if len(s) > 1:
                result = [result]
                append = result.append
                s = iter(s)
                next = s.next
                next()
                for special in s:
                    nonmatch = next()
                    remap = quoted_mapper(special)
                    if remap is None:
                        if len(special) == 6:
                            uni = int(special[2:], 16)
                            if 0xd800 <= uni <= 0xdbff and allow_double:
                                uni, nonmatch = parse_double_unicode(uni, nonmatch, next, token)
                            remap = parse_encoded_chr(uni)
                        else:
                            return badstring(token, special)
                    append(remap)
                    append(parse_quoted_str(token, nonmatch))

                result = parse_join_str(result)
            if cachestrings:
                result = token[-1].stringcache(result, result)
            return result


        def parse_double_unicode(uni, nonmatch, next, token):
            ''' Munged version of UCS-4 code pair stuff from
                simplejson.
            '''
            ok = True
            try:
                uni2 = next()
                nonmatch2 = next()
            except:
                ok = False
            ok = ok and not nonmatch and uni2.startswith(r'\u') and len(uni2) == 6
            if ok:
                nonmatch = uni2
                uni = 0x10000 + (((uni - 0xd800) << 10) | (int(uni2[2:], 16) - 0xdc00))
                return uni, nonmatch2
            token[-1].error('Invalid second ch in double sequence: %s' % repr(nonmatch), token)

        return parse

    @staticmethod
    def triplequoted(token):
        tokens = token[-1]
        source = tokens.source
        result = []
        start = 3 - token[0]
        while 1:
            end = source.find('"""', start)
            if end < 0:
                tokens.error('Did not find end for triple-quoted string', token)
            if source[end-1] != '\\':
                break
            result.append(source[start:end-1])
            result.append('"""')
            start = end + 3
        result.append(source[start:end])
        offset = bisect.bisect(tokens, (- end -2, ))
        tokens[offset:] = []
        return ''.join(result)


class UnquotedToken(object):
    ''' Subclass or replace this if you don't like the unquoted
        token handling.  This is designed to be a superset of JSON:

          - Integers allowed to be expressed in octal, binary, or hex
            as well as decimal.

          - Integers can have embedded underscores.

          - Non-match of a special token will just be wrapped as a unicode
            string.

          - Numbers can be preceded by '+' as well s '-'
          - Numbers can be left-zero-filled
          - If a decimal point is present, digits are required on either side,
            but not both sides
    '''

    use_decimal = False
    parse_int = staticmethod(
        lambda s: int(s.replace('_', ''), 0))
    parse_float = float
    parse_unquoted_str = staticmethod(
        lambda token, unicode=unicode: unicode(token[2], 'utf-8'))

    special_strings = dict(true = True, false = False, null = None)

    unquoted_pattern = r'''
    (?:
        true | false | null       |     # Special JSON names
        (?P<num>
            [-+]?                       # Optional sign
            (?:
                0[xX](_*[0-9a-fA-F]+)+   | # Hex integer
                0[bB](_*[01]+)+          | # binary integer
                0[oO](_*[0-7]+)+         | # Octal integer
                \d+(_*\d+)*              | # Decimal integer
                (?P<float>
                    (?:
                      \d+(\.\d*)? |     # One or more digits,
                                        # optional frac
                      \.\d+             # Leading decimal point
                    )
                    (?:[eE][-+]?\d+)?   # Optional exponent
                )
            )
        )
    )  \Z                               # Match end of string
    '''

    def unquoted_parse_factory(self):
        unquoted_match = re.compile(self.unquoted_pattern,
                        re.VERBOSE).match

        parse_unquoted_str = self.parse_unquoted_str
        parse_float = self.parse_float
        parse_int = self.parse_int
        special = self.special_strings

        if self.use_decimal:
            from decimal import Decimal
            parse_float = Decimal

        def parse(token, next):
            s = token[2]
            m = unquoted_match(s)
            if m is None:
                return parse_unquoted_str(token)
            if m.group('num') is None:
                return special[s]
            if m.group('float') is None:
                return parse_int(s.replace('_', ''))
            return parse_float(s)

        return parse


class EqualToken(object):
    ''' Subclass or replace this if you don't like the = string handling
    '''

    encode_equals_str = None

    @staticmethod
    def parse_equals(stringlist, indent, token):
        ''' token probably not needed except maybe for error reporting.
            Replace this with something that does what you want.
        '''
        # Strip any trailing whitespace to the right
        stringlist = [x.rstrip() for x in stringlist]

        # Strip any embedded comments
        stringlist = [x for x in stringlist if x.startswith(indent) or not x]

        # Strip trailing whitespace down below
        while stringlist and not stringlist[-1]:
            stringlist.pop()

        # Special cases for single line
        if not stringlist:
            return ''
        if len(stringlist) == 1:
            return stringlist[0].strip()

        # Strip whitespace on first line
        if stringlist and not stringlist[0]:
            stringlist.pop(0)

        # Dedent all the strings to one past the equals
        dedent = len(indent)
        stringlist = [x[dedent:] for x in stringlist]

        # Figure out if should dedent one more
        if min((not x and 500 or len(x) - len(x.lstrip())) for x in stringlist):
            stringlist = [x[1:] for x in stringlist]

        # Give every line its own linefeed (keeps later parsing from
        # treating this as a number, for example)
        stringlist.append('')

        # Return all joined up as a single unicode string
        return '\n'.join(stringlist)

    def equal_parse_factory(self, read_unquoted):

        parse_equals = self.parse_equals
        encoder = self.encode_equals_str

        if encoder is None:
            encoder = read_unquoted

        def parse(firsttok, next):
            tokens = firsttok[-1]
            indent, linenum = firsttok[4:6]
            token = next()
            while token[5] == linenum:
                token = next()
            while  token[4] > indent:
                token = next()
            tokens.push(token)

            # Get rid of \n, and indent one past =
            indent = indent[1:] + ' '

            bigstring = tokens.source[-firsttok[0] + 1 : -token[0]]
            stringlist = bigstring.split('\n')
            stringlist[0] = indent + stringlist[0]
            token = list(firsttok)
            token[1:3] = '=', parse_equals(stringlist, indent, firsttok)
            return encoder(token, next)

        return parse


class RsonParser(object):
    ''' Parser for RSON
    '''

    disallow_trailing_commas = True
    disallow_rson_sublists = False
    disallow_rson_subdicts = False

    @staticmethod
    def post_parse(tokens, value):
        return value

    def client_info(self, parse_locals):
        pass

    def parser_factory(self, len=len, type=type, isinstance=isinstance, list=list, basestring=basestring):

        Tokenizer = self.Tokenizer
        tokenizer = Tokenizer.factory()
        error = Tokenizer.error

        read_unquoted = self.unquoted_parse_factory()
        read_quoted = self.quoted_parse_factory()
        parse_equals = self.equal_parse_factory(read_unquoted)
        new_object, new_array = self.object_type_factory()
        disallow_trailing_commas = self.disallow_trailing_commas
        disallow_missing_object_keys = self.disallow_missing_object_keys
        key_handling = [disallow_missing_object_keys, self.disallow_multiple_object_keys]
        disallow_nonstring_keys = self.disallow_nonstring_keys
        post_parse = self.post_parse


        def bad_array_element(token, next):
            error('Expected array element', token)

        def bad_dict_key(token, next):
            error('Expected dictionary key', token)

        def bad_dict_value(token, next):
            error('Expected dictionary value', token)

        def bad_top_value(token, next):
            error('Expected start of object', token)

        def bad_unindent(token, next):
            error('Unindent does not match any outer indentation level', token)

        def bad_indent(token, next):
            error('Unexpected indentation', token)

        def read_json_array(firsttok, next):
            result = new_array([], firsttok)
            append = result.append
            while 1:
                token = next()
                t0 = token[1]
                if t0 == ']':
                    if result and disallow_trailing_commas:
                        error('Unexpected trailing comma', token)
                    break
                append(json_value_dispatch(t0,  bad_array_element)(token, next))
                delim = next()
                t0 = delim[1]
                if t0 == ',':
                    continue
                if t0 != ']':
                    if t0 == '@':
                        error('Unterminated list (no matching "]")', firsttok)
                    error('Expected "," or "]"', delim)
                break
            return result

        def read_json_dict(firsttok, next):
            result = new_object()
            append = result.append
            while 1:
                token = next()
                t0 = token[1]
                if t0  == '}':
                    if result and disallow_trailing_commas:
                        error('Unexpected trailing comma', token)
                    break
                key = json_value_dispatch(t0, bad_dict_key)(token, next)
                if disallow_nonstring_keys and not isinstance(key, basestring):
                    error('Non-string key %s not supported' % repr(key), token)
                token = next()
                t0 = token[1]
                if t0 != ':':
                    error('Expected ":" after dict key %s' % repr(key), token)
                token = next()
                t0 = token[1]
                value = json_value_dispatch(t0, bad_dict_value)(token, next)
                append([key, value])
                delim = next()
                t0 = delim[1]
                if t0 == ',':
                    continue
                if t0 != '}':
                    if t0 == '@':
                        error('Unterminated dict (no matching "}")', firsttok)
                    error('Expected "," or "}"', delim)
                break
            return result.get_result(firsttok)

        def read_rson_unquoted(firsttok, next):
            toklist = []
            linenum = firsttok[5]
            while 1:
                token = next()
                if token[5] != linenum or token[1] in ':=':
                    break
                toklist.append(token)
            firsttok[-1].push(token)
            if not toklist:
                return read_unquoted(firsttok, next)
            s = list(firsttok[2:4])
            for tok in toklist:
                s.extend(tok[2:4])
            result = list(firsttok)
            result[3] = s.pop()
            result[2] = ''.join(s)
            return read_unquoted(result, next)

        json_value_dispatch = {'X':read_unquoted, '[':read_json_array,
                               '{': read_json_dict, '"':read_quoted}.get


        rson_value_dispatch = {'X':read_rson_unquoted, '[':read_json_array,
                                  '{': read_json_dict, '"':read_quoted,
                                   '=': parse_equals}

        if self.disallow_rson_sublists:
            rson_value_dispatch['['] = read_rson_unquoted

        if self.disallow_rson_subdicts:
            rson_value_dispatch['{'] = read_rson_unquoted

        rson_key_dispatch = rson_value_dispatch.copy()
        if disallow_missing_object_keys:
            del rson_key_dispatch['=']

        rson_value_dispatch = rson_value_dispatch.get
        rson_key_dispatch = rson_key_dispatch.get

        empty_object = new_object().get_result(None)
        empty_array = new_array([], None)
        empty_array_type = type(empty_array)
        empties = empty_object, empty_array

        def parse_recurse_array(stack, next, token, result):
            arrayindent, linenum = stack[-1][4:6]
            linenum -= not result
            while 1:
                thisindent, newlinenum = token[4:6]
                if thisindent != arrayindent:
                    if thisindent < arrayindent:
                        return result, token
                    if result:
                        stack.append(token)
                        lastitem = result[-1]
                        if lastitem == empty_array:
                            result[-1], token = parse_recurse_array(stack, next, token, lastitem)
                        elif lastitem == empty_object:
                            result[-1], token = parse_recurse_dict(stack, next, token, lastitem)
                        else:
                            result = None
                    if result:
                        stack.pop()
                        thisindent, newlinenum = token[4:6]
                        if thisindent <= arrayindent:
                            continue
                        bad_unindent(token, next)
                    bad_indent(token, next)
                if newlinenum <= linenum:
                    if token[1] in '=:':
                        error('Cannot mix list elements with dict (key/value) elements', token)
                    error('Array elements must either be on separate lines or enclosed in []', token)
                linenum = newlinenum
                value = rson_value_dispatch(token[1], bad_top_value)(token, next)
                result.append(value)
                token = next()

        def parse_one_dict_entry(stack, next, token, entry, mydict):
            arrayindent, linenum = stack[-1][4:6]
            while token[1] == ':':
                tok1 = next()
                thisindent, newlinenum = tok1[4:6]
                if newlinenum == linenum:
                    value = rson_value_dispatch(tok1[1], bad_top_value)(tok1, next)
                    token = next()
                    entry.append(value)
                    continue
                if thisindent <= arrayindent:
                    error('Expected indented line after ":"', token)
                token = tok1

            if not entry:
                error('Expected key', token)

            thisindent, newlinenum = token[4:6]
            if newlinenum == linenum and token[1] == '=':
                value = rson_value_dispatch(token[1], bad_top_value)(token, next)
                entry.append(value)
                token = next()
            elif thisindent > arrayindent:
                stack.append(token)
                value, token = parse_recurse(stack, next)
                if entry[-1] in empties:
                    if type(entry[-1]) is type(value):
                        entry[-1] = value
                    else:
                        error('Cannot load %s into %s' % (type(value), type(entry[-1])), stack[-1])
                elif len(value) == 1 and type(value) is empty_array_type:
                    entry.extend(value)
                else:
                    entry.append(value)
                stack.pop()
            length = len(entry)
            if length != 2  and key_handling[length > 2]:
                if length < 2:
                    error('Expected ":" or "=", or indented line', token)
                error("rson client's object handlers do not support chained objects", token)
            if disallow_nonstring_keys:
                for key in entry[:-1]:
                    if not isinstance(key, basestring):
                        error('Non-string key %s not supported' % repr(key), token)
            mydict.append(entry)
            return token

        def parse_recurse_dict(stack, next, token, result):
            arrayindent = stack[-1][4]
            while 1:
                thisindent = token[4]
                if thisindent != arrayindent:
                    if thisindent < arrayindent:
                        return result.get_result(token), token
                    bad_unindent(token, next)
                key = rson_key_dispatch(token[1], bad_top_value)(token, next)
                stack[-1] = token
                token = parse_one_dict_entry(stack, next, next(), [key], result)

        def parse_recurse(stack, next, tokens=None):
            ''' parse_recurse ALWAYS returns a list or a dict.
                (or the user variants thereof)
                It is up to the caller to determine that it was an array
                of length 1 and strip the contents out of the array.
            '''
            firsttok = stack[-1]
            value = rson_value_dispatch(firsttok[1], bad_top_value)(firsttok, next)

            # We return an array if the next value is on a new line and either
            # is at the same indentation, or the current value is an empty list

            token = next()
            if (token[5] != firsttok[5] and
                    (token[4] <= firsttok[4] or
                     value in empties) and disallow_missing_object_keys):
                result = new_array([value], firsttok)
                if tokens is not None:
                    tokens.top_object = result
                return parse_recurse_array(stack, next, token, result)

            # Otherwise, return a dict
            result = new_object()
            if tokens is not None:
                tokens.top_object = result
            token = parse_one_dict_entry(stack, next, token, [value], result)
            return parse_recurse_dict(stack, next, token, result)


        def parse(source):
            tokens = tokenizer(source, None)
            tokens.stringcache = {}.setdefault
            tokens.client_info = client_info
            next = tokens.next
            value, token = parse_recurse([next()], next, tokens)
            if token[1] != '@':
                error('Unexpected additional data', token)

            # If it's a single item and we don't have a specialized
            # object builder, just strip the outer list.
            if (len(value) == 1 and isinstance(value, list)
                   and disallow_missing_object_keys):
                value = value[0]
            return post_parse(tokens, value)

        client_info = self.client_info(locals())

        return parse


class RsonSystem(RsonParser, UnquotedToken, QuotedToken, EqualToken, Dispatcher, BaseObjects):
    Tokenizer = Tokenizer

loads = RsonSystem.dispatcher_factory()
