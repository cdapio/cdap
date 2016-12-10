/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/*
 * Copyright (c) 2015-2016, Matthieu Monsch.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

 /**
  * Modification by Edwin Elia
  * Avro specification does not support MapType with key that is non string type.
  * However, CDAP does not enforce this, therefore I am annotating avsc library to support it
  **/

 (function(f){if(typeof exports==="object"&&typeof module!=="undefined"){module.exports=f()}else if(typeof define==="function"&&define.amd){define([],f)}else{var g;if(typeof window!=="undefined"){g=window}else if(typeof global!=="undefined"){g=global}else if(typeof self!=="undefined"){g=self}else{g=this}g.avsc = f()}})(function(){var define,module,exports;return (function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
'use strict'

exports.toByteArray = toByteArray
exports.fromByteArray = fromByteArray

var lookup = []
var revLookup = []
var Arr = typeof Uint8Array !== 'undefined' ? Uint8Array : Array

function init () {
  var code = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
  for (var i = 0, len = code.length; i < len; ++i) {
    lookup[i] = code[i]
    revLookup[code.charCodeAt(i)] = i
  }

  revLookup['-'.charCodeAt(0)] = 62
  revLookup['_'.charCodeAt(0)] = 63
}

init()

function toByteArray (b64) {
  var i, j, l, tmp, placeHolders, arr
  var len = b64.length

  if (len % 4 > 0) {
    throw new Error('Invalid string. Length must be a multiple of 4')
  }

  // the number of equal signs (place holders)
  // if there are two placeholders, than the two characters before it
  // represent one byte
  // if there is only one, then the three characters before it represent 2 bytes
  // this is just a cheap hack to not do indexOf twice
  placeHolders = b64[len - 2] === '=' ? 2 : b64[len - 1] === '=' ? 1 : 0

  // base64 is 4/3 + up to two characters of the original data
  arr = new Arr(len * 3 / 4 - placeHolders)

  // if there are placeholders, only get up to the last complete 4 chars
  l = placeHolders > 0 ? len - 4 : len

  var L = 0

  for (i = 0, j = 0; i < l; i += 4, j += 3) {
    tmp = (revLookup[b64.charCodeAt(i)] << 18) | (revLookup[b64.charCodeAt(i + 1)] << 12) | (revLookup[b64.charCodeAt(i + 2)] << 6) | revLookup[b64.charCodeAt(i + 3)]
    arr[L++] = (tmp >> 16) & 0xFF
    arr[L++] = (tmp >> 8) & 0xFF
    arr[L++] = tmp & 0xFF
  }

  if (placeHolders === 2) {
    tmp = (revLookup[b64.charCodeAt(i)] << 2) | (revLookup[b64.charCodeAt(i + 1)] >> 4)
    arr[L++] = tmp & 0xFF
  } else if (placeHolders === 1) {
    tmp = (revLookup[b64.charCodeAt(i)] << 10) | (revLookup[b64.charCodeAt(i + 1)] << 4) | (revLookup[b64.charCodeAt(i + 2)] >> 2)
    arr[L++] = (tmp >> 8) & 0xFF
    arr[L++] = tmp & 0xFF
  }

  return arr
}

function tripletToBase64 (num) {
  return lookup[num >> 18 & 0x3F] + lookup[num >> 12 & 0x3F] + lookup[num >> 6 & 0x3F] + lookup[num & 0x3F]
}

function encodeChunk (uint8, start, end) {
  var tmp
  var output = []
  for (var i = start; i < end; i += 3) {
    tmp = (uint8[i] << 16) + (uint8[i + 1] << 8) + (uint8[i + 2])
    output.push(tripletToBase64(tmp))
  }
  return output.join('')
}

function fromByteArray (uint8) {
  var tmp
  var len = uint8.length
  var extraBytes = len % 3 // if we have 1 byte left, pad 2 bytes
  var output = ''
  var parts = []
  var maxChunkLength = 16383 // must be multiple of 3

  // go through the array every three bytes, we'll deal with trailing stuff later
  for (var i = 0, len2 = len - extraBytes; i < len2; i += maxChunkLength) {
    parts.push(encodeChunk(uint8, i, (i + maxChunkLength) > len2 ? len2 : (i + maxChunkLength)))
  }

  // pad the end with zeros, but make sure to not forget the extra bytes
  if (extraBytes === 1) {
    tmp = uint8[len - 1]
    output += lookup[tmp >> 2]
    output += lookup[(tmp << 4) & 0x3F]
    output += '=='
  } else if (extraBytes === 2) {
    tmp = (uint8[len - 2] << 8) + (uint8[len - 1])
    output += lookup[tmp >> 10]
    output += lookup[(tmp >> 4) & 0x3F]
    output += lookup[(tmp << 2) & 0x3F]
    output += '='
  }

  parts.push(output)

  return parts.join('')
}

},{}],2:[function(require,module,exports){

},{}],3:[function(require,module,exports){
(function (global){
'use strict';

var buffer = require('buffer');
var Buffer = buffer.Buffer;
var SlowBuffer = buffer.SlowBuffer;
var MAX_LEN = buffer.kMaxLength || 2147483647;
exports.alloc = function alloc(size, fill, encoding) {
  if (typeof Buffer.alloc === 'function') {
    return Buffer.alloc(size, fill, encoding);
  }
  if (typeof encoding === 'number') {
    throw new TypeError('encoding must not be number');
  }
  if (typeof size !== 'number') {
    throw new TypeError('size must be a number');
  }
  if (size > MAX_LEN) {
    throw new RangeError('size is too large');
  }
  var enc = encoding;
  var _fill = fill;
  if (_fill === undefined) {
    enc = undefined;
    _fill = 0;
  }
  var buf = new Buffer(size);
  if (typeof _fill === 'string') {
    var fillBuf = new Buffer(_fill, enc);
    var flen = fillBuf.length;
    var i = -1;
    while (++i < size) {
      buf[i] = fillBuf[i % flen];
    }
  } else {
    buf.fill(_fill);
  }
  return buf;
}
exports.allocUnsafe = function allocUnsafe(size) {
  if (typeof Buffer.allocUnsafe === 'function') {
    return Buffer.allocUnsafe(size);
  }
  if (typeof size !== 'number') {
    throw new TypeError('size must be a number');
  }
  if (size > MAX_LEN) {
    throw new RangeError('size is too large');
  }
  return new Buffer(size);
}
exports.from = function from(value, encodingOrOffset, length) {
  if (typeof Buffer.from === 'function' && (!global.Uint8Array || Uint8Array.from !== Buffer.from)) {
    return Buffer.from(value, encodingOrOffset, length);
  }
  if (typeof value === 'number') {
    throw new TypeError('"value" argument must not be a number');
  }
  if (typeof value === 'string') {
    return new Buffer(value, encodingOrOffset);
  }
  if (typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer) {
    var offset = encodingOrOffset;
    if (arguments.length === 1) {
      return new Buffer(value);
    }
    if (typeof offset === 'undefined') {
      offset = 0;
    }
    var len = length;
    if (typeof len === 'undefined') {
      len = value.byteLength - offset;
    }
    if (offset >= value.byteLength) {
      throw new RangeError('\'offset\' is out of bounds');
    }
    if (len > value.byteLength - offset) {
      throw new RangeError('\'length\' is out of bounds');
    }
    return new Buffer(value.slice(offset, offset + len));
  }
  if (Buffer.isBuffer(value)) {
    var out = new Buffer(value.length);
    value.copy(out, 0, 0, value.length);
    return out;
  }
  if (value) {
    if (Array.isArray(value) || (typeof ArrayBuffer !== 'undefined' && value.buffer instanceof ArrayBuffer) || 'length' in value) {
      return new Buffer(value);
    }
    if (value.type === 'Buffer' && Array.isArray(value.data)) {
      return new Buffer(value.data);
    }
  }

  throw new TypeError('First argument must be a string, Buffer, ' + 'ArrayBuffer, Array, or array-like object.');
}
exports.allocUnsafeSlow = function allocUnsafeSlow(size) {
  if (typeof Buffer.allocUnsafeSlow === 'function') {
    return Buffer.allocUnsafeSlow(size);
  }
  if (typeof size !== 'number') {
    throw new TypeError('size must be a number');
  }
  if (size >= MAX_LEN) {
    throw new RangeError('size is too large');
  }
  return new SlowBuffer(size);
}

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{"buffer":4}],4:[function(require,module,exports){
(function (global){
/*!
 * The buffer module from node.js, for the browser.
 *
 * @author   Feross Aboukhadijeh <feross@feross.org> <http://feross.org>
 * @license  MIT
 */
/* eslint-disable no-proto */

'use strict'

var base64 = require('base64-js')
var ieee754 = require('ieee754')
var isArray = require('isarray')

exports.Buffer = Buffer
exports.SlowBuffer = SlowBuffer
exports.INSPECT_MAX_BYTES = 50

/**
 * If `Buffer.TYPED_ARRAY_SUPPORT`:
 *   === true    Use Uint8Array implementation (fastest)
 *   === false   Use Object implementation (most compatible, even IE6)
 *
 * Browsers that support typed arrays are IE 10+, Firefox 4+, Chrome 7+, Safari 5.1+,
 * Opera 11.6+, iOS 4.2+.
 *
 * Due to various browser bugs, sometimes the Object implementation will be used even
 * when the browser supports typed arrays.
 *
 * Note:
 *
 *   - Firefox 4-29 lacks support for adding new properties to `Uint8Array` instances,
 *     See: https://bugzilla.mozilla.org/show_bug.cgi?id=695438.
 *
 *   - Chrome 9-10 is missing the `TypedArray.prototype.subarray` function.
 *
 *   - IE10 has a broken `TypedArray.prototype.subarray` function which returns arrays of
 *     incorrect length in some situations.

 * We detect these buggy browsers and set `Buffer.TYPED_ARRAY_SUPPORT` to `false` so they
 * get the Object implementation, which is slower but behaves correctly.
 */
Buffer.TYPED_ARRAY_SUPPORT = global.TYPED_ARRAY_SUPPORT !== undefined
  ? global.TYPED_ARRAY_SUPPORT
  : typedArraySupport()

/*
 * Export kMaxLength after typed array support is determined.
 */
exports.kMaxLength = kMaxLength()

function typedArraySupport () {
  try {
    var arr = new Uint8Array(1)
    arr.__proto__ = {__proto__: Uint8Array.prototype, foo: function () { return 42 }}
    return arr.foo() === 42 && // typed array instances can be augmented
        typeof arr.subarray === 'function' && // chrome 9-10 lack `subarray`
        arr.subarray(1, 1).byteLength === 0 // ie10 has broken `subarray`
  } catch (e) {
    return false
  }
}

function kMaxLength () {
  return Buffer.TYPED_ARRAY_SUPPORT
    ? 0x7fffffff
    : 0x3fffffff
}

function createBuffer (that, length) {
  if (kMaxLength() < length) {
    throw new RangeError('Invalid typed array length')
  }
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    // Return an augmented `Uint8Array` instance, for best performance
    that = new Uint8Array(length)
    that.__proto__ = Buffer.prototype
  } else {
    // Fallback: Return an object instance of the Buffer class
    if (that === null) {
      that = new Buffer(length)
    }
    that.length = length
  }

  return that
}

/**
 * The Buffer constructor returns instances of `Uint8Array` that have their
 * prototype changed to `Buffer.prototype`. Furthermore, `Buffer` is a subclass of
 * `Uint8Array`, so the returned instances will have all the node `Buffer` methods
 * and the `Uint8Array` methods. Square bracket notation works as expected -- it
 * returns a single octet.
 *
 * The `Uint8Array` prototype remains unmodified.
 */

function Buffer (arg, encodingOrOffset, length) {
  if (!Buffer.TYPED_ARRAY_SUPPORT && !(this instanceof Buffer)) {
    return new Buffer(arg, encodingOrOffset, length)
  }

  // Common case.
  if (typeof arg === 'number') {
    if (typeof encodingOrOffset === 'string') {
      throw new Error(
        'If encoding is specified then the first argument must be a string'
      )
    }
    return allocUnsafe(this, arg)
  }
  return from(this, arg, encodingOrOffset, length)
}

Buffer.poolSize = 8192 // not used by this implementation

// TODO: Legacy, not needed anymore. Remove in next major version.
Buffer._augment = function (arr) {
  arr.__proto__ = Buffer.prototype
  return arr
}

function from (that, value, encodingOrOffset, length) {
  if (typeof value === 'number') {
    throw new TypeError('"value" argument must not be a number')
  }

  if (typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer) {
    return fromArrayBuffer(that, value, encodingOrOffset, length)
  }

  if (typeof value === 'string') {
    return fromString(that, value, encodingOrOffset)
  }

  return fromObject(that, value)
}

/**
 * Functionally equivalent to Buffer(arg, encoding) but throws a TypeError
 * if value is a number.
 * Buffer.from(str[, encoding])
 * Buffer.from(array)
 * Buffer.from(buffer)
 * Buffer.from(arrayBuffer[, byteOffset[, length]])
 **/
Buffer.from = function (value, encodingOrOffset, length) {
  return from(null, value, encodingOrOffset, length)
}

if (Buffer.TYPED_ARRAY_SUPPORT) {
  Buffer.prototype.__proto__ = Uint8Array.prototype
  Buffer.__proto__ = Uint8Array
  if (typeof Symbol !== 'undefined' && Symbol.species &&
      Buffer[Symbol.species] === Buffer) {
    // Fix subarray() in ES2016. See: https://github.com/feross/buffer/pull/97
    Object.defineProperty(Buffer, Symbol.species, {
      value: null,
      configurable: true
    })
  }
}

function assertSize (size) {
  if (typeof size !== 'number') {
    throw new TypeError('"size" argument must be a number')
  }
}

function alloc (that, size, fill, encoding) {
  assertSize(size)
  if (size <= 0) {
    return createBuffer(that, size)
  }
  if (fill !== undefined) {
    // Only pay attention to encoding if it's a string. This
    // prevents accidentally sending in a number that would
    // be interpretted as a start offset.
    return typeof encoding === 'string'
      ? createBuffer(that, size).fill(fill, encoding)
      : createBuffer(that, size).fill(fill)
  }
  return createBuffer(that, size)
}

/**
 * Creates a new filled Buffer instance.
 * alloc(size[, fill[, encoding]])
 **/
Buffer.alloc = function (size, fill, encoding) {
  return alloc(null, size, fill, encoding)
}

function allocUnsafe (that, size) {
  assertSize(size)
  that = createBuffer(that, size < 0 ? 0 : checked(size) | 0)
  if (!Buffer.TYPED_ARRAY_SUPPORT) {
    for (var i = 0; i < size; ++i) {
      that[i] = 0
    }
  }
  return that
}

/**
 * Equivalent to Buffer(num), by default creates a non-zero-filled Buffer instance.
 * */
Buffer.allocUnsafe = function (size) {
  return allocUnsafe(null, size)
}
/**
 * Equivalent to SlowBuffer(num), by default creates a non-zero-filled Buffer instance.
 */
Buffer.allocUnsafeSlow = function (size) {
  return allocUnsafe(null, size)
}

function fromString (that, string, encoding) {
  if (typeof encoding !== 'string' || encoding === '') {
    encoding = 'utf8'
  }

  if (!Buffer.isEncoding(encoding)) {
    throw new TypeError('"encoding" must be a valid string encoding')
  }

  var length = byteLength(string, encoding) | 0
  that = createBuffer(that, length)

  that.write(string, encoding)
  return that
}

function fromArrayLike (that, array) {
  var length = checked(array.length) | 0
  that = createBuffer(that, length)
  for (var i = 0; i < length; i += 1) {
    that[i] = array[i] & 255
  }
  return that
}

function fromArrayBuffer (that, array, byteOffset, length) {
  array.byteLength // this throws if `array` is not a valid ArrayBuffer

  if (byteOffset < 0 || array.byteLength < byteOffset) {
    throw new RangeError('\'offset\' is out of bounds')
  }

  if (array.byteLength < byteOffset + (length || 0)) {
    throw new RangeError('\'length\' is out of bounds')
  }

  if (length === undefined) {
    array = new Uint8Array(array, byteOffset)
  } else {
    array = new Uint8Array(array, byteOffset, length)
  }

  if (Buffer.TYPED_ARRAY_SUPPORT) {
    // Return an augmented `Uint8Array` instance, for best performance
    that = array
    that.__proto__ = Buffer.prototype
  } else {
    // Fallback: Return an object instance of the Buffer class
    that = fromArrayLike(that, array)
  }
  return that
}

function fromObject (that, obj) {
  if (Buffer.isBuffer(obj)) {
    var len = checked(obj.length) | 0
    that = createBuffer(that, len)

    if (that.length === 0) {
      return that
    }

    obj.copy(that, 0, 0, len)
    return that
  }

  if (obj) {
    if ((typeof ArrayBuffer !== 'undefined' &&
        obj.buffer instanceof ArrayBuffer) || 'length' in obj) {
      if (typeof obj.length !== 'number' || isnan(obj.length)) {
        return createBuffer(that, 0)
      }
      return fromArrayLike(that, obj)
    }

    if (obj.type === 'Buffer' && isArray(obj.data)) {
      return fromArrayLike(that, obj.data)
    }
  }

  throw new TypeError('First argument must be a string, Buffer, ArrayBuffer, Array, or array-like object.')
}

function checked (length) {
  // Note: cannot use `length < kMaxLength` here because that fails when
  // length is NaN (which is otherwise coerced to zero.)
  if (length >= kMaxLength()) {
    throw new RangeError('Attempt to allocate Buffer larger than maximum ' +
                         'size: 0x' + kMaxLength().toString(16) + ' bytes')
  }
  return length | 0
}

function SlowBuffer (length) {
  if (+length != length) { // eslint-disable-line eqeqeq
    length = 0
  }
  return Buffer.alloc(+length)
}

Buffer.isBuffer = function isBuffer (b) {
  return !!(b != null && b._isBuffer)
}

Buffer.compare = function compare (a, b) {
  if (!Buffer.isBuffer(a) || !Buffer.isBuffer(b)) {
    throw new TypeError('Arguments must be Buffers')
  }

  if (a === b) return 0

  var x = a.length
  var y = b.length

  for (var i = 0, len = Math.min(x, y); i < len; ++i) {
    if (a[i] !== b[i]) {
      x = a[i]
      y = b[i]
      break
    }
  }

  if (x < y) return -1
  if (y < x) return 1
  return 0
}

Buffer.isEncoding = function isEncoding (encoding) {
  switch (String(encoding).toLowerCase()) {
    case 'hex':
    case 'utf8':
    case 'utf-8':
    case 'ascii':
    case 'binary':
    case 'base64':
    case 'raw':
    case 'ucs2':
    case 'ucs-2':
    case 'utf16le':
    case 'utf-16le':
      return true
    default:
      return false
  }
}

Buffer.concat = function concat (list, length) {
  if (!isArray(list)) {
    throw new TypeError('"list" argument must be an Array of Buffers')
  }

  if (list.length === 0) {
    return Buffer.alloc(0)
  }

  var i
  if (length === undefined) {
    length = 0
    for (i = 0; i < list.length; ++i) {
      length += list[i].length
    }
  }

  var buffer = Buffer.allocUnsafe(length)
  var pos = 0
  for (i = 0; i < list.length; ++i) {
    var buf = list[i]
    if (!Buffer.isBuffer(buf)) {
      throw new TypeError('"list" argument must be an Array of Buffers')
    }
    buf.copy(buffer, pos)
    pos += buf.length
  }
  return buffer
}

function byteLength (string, encoding) {
  if (Buffer.isBuffer(string)) {
    return string.length
  }
  if (typeof ArrayBuffer !== 'undefined' && typeof ArrayBuffer.isView === 'function' &&
      (ArrayBuffer.isView(string) || string instanceof ArrayBuffer)) {
    return string.byteLength
  }
  if (typeof string !== 'string') {
    string = '' + string
  }

  var len = string.length
  if (len === 0) return 0

  // Use a for loop to avoid recursion
  var loweredCase = false
  for (;;) {
    switch (encoding) {
      case 'ascii':
      case 'binary':
      case 'raw':
      case 'raws':
        return len
      case 'utf8':
      case 'utf-8':
      case undefined:
        return utf8ToBytes(string).length
      case 'ucs2':
      case 'ucs-2':
      case 'utf16le':
      case 'utf-16le':
        return len * 2
      case 'hex':
        return len >>> 1
      case 'base64':
        return base64ToBytes(string).length
      default:
        if (loweredCase) return utf8ToBytes(string).length // assume utf8
        encoding = ('' + encoding).toLowerCase()
        loweredCase = true
    }
  }
}
Buffer.byteLength = byteLength

function slowToString (encoding, start, end) {
  var loweredCase = false

  // No need to verify that "this.length <= MAX_UINT32" since it's a read-only
  // property of a typed array.

  // This behaves neither like String nor Uint8Array in that we set start/end
  // to their upper/lower bounds if the value passed is out of range.
  // undefined is handled specially as per ECMA-262 6th Edition,
  // Section 13.3.3.7 Runtime Semantics: KeyedBindingInitialization.
  if (start === undefined || start < 0) {
    start = 0
  }
  // Return early if start > this.length. Done here to prevent potential uint32
  // coercion fail below.
  if (start > this.length) {
    return ''
  }

  if (end === undefined || end > this.length) {
    end = this.length
  }

  if (end <= 0) {
    return ''
  }

  // Force coersion to uint32. This will also coerce falsey/NaN values to 0.
  end >>>= 0
  start >>>= 0

  if (end <= start) {
    return ''
  }

  if (!encoding) encoding = 'utf8'

  while (true) {
    switch (encoding) {
      case 'hex':
        return hexSlice(this, start, end)

      case 'utf8':
      case 'utf-8':
        return utf8Slice(this, start, end)

      case 'ascii':
        return asciiSlice(this, start, end)

      case 'binary':
        return binarySlice(this, start, end)

      case 'base64':
        return base64Slice(this, start, end)

      case 'ucs2':
      case 'ucs-2':
      case 'utf16le':
      case 'utf-16le':
        return utf16leSlice(this, start, end)

      default:
        if (loweredCase) throw new TypeError('Unknown encoding: ' + encoding)
        encoding = (encoding + '').toLowerCase()
        loweredCase = true
    }
  }
}

// The property is used by `Buffer.isBuffer` and `is-buffer` (in Safari 5-7) to detect
// Buffer instances.
Buffer.prototype._isBuffer = true

function swap (b, n, m) {
  var i = b[n]
  b[n] = b[m]
  b[m] = i
}

Buffer.prototype.swap16 = function swap16 () {
  var len = this.length
  if (len % 2 !== 0) {
    throw new RangeError('Buffer size must be a multiple of 16-bits')
  }
  for (var i = 0; i < len; i += 2) {
    swap(this, i, i + 1)
  }
  return this
}

Buffer.prototype.swap32 = function swap32 () {
  var len = this.length
  if (len % 4 !== 0) {
    throw new RangeError('Buffer size must be a multiple of 32-bits')
  }
  for (var i = 0; i < len; i += 4) {
    swap(this, i, i + 3)
    swap(this, i + 1, i + 2)
  }
  return this
}

Buffer.prototype.toString = function toString () {
  var length = this.length | 0
  if (length === 0) return ''
  if (arguments.length === 0) return utf8Slice(this, 0, length)
  return slowToString.apply(this, arguments)
}

Buffer.prototype.equals = function equals (b) {
  if (!Buffer.isBuffer(b)) throw new TypeError('Argument must be a Buffer')
  if (this === b) return true
  return Buffer.compare(this, b) === 0
}

Buffer.prototype.inspect = function inspect () {
  var str = ''
  var max = exports.INSPECT_MAX_BYTES
  if (this.length > 0) {
    str = this.toString('hex', 0, max).match(/.{2}/g).join(' ')
    if (this.length > max) str += ' ... '
  }
  return '<Buffer ' + str + '>'
}

Buffer.prototype.compare = function compare (target, start, end, thisStart, thisEnd) {
  if (!Buffer.isBuffer(target)) {
    throw new TypeError('Argument must be a Buffer')
  }

  if (start === undefined) {
    start = 0
  }
  if (end === undefined) {
    end = target ? target.length : 0
  }
  if (thisStart === undefined) {
    thisStart = 0
  }
  if (thisEnd === undefined) {
    thisEnd = this.length
  }

  if (start < 0 || end > target.length || thisStart < 0 || thisEnd > this.length) {
    throw new RangeError('out of range index')
  }

  if (thisStart >= thisEnd && start >= end) {
    return 0
  }
  if (thisStart >= thisEnd) {
    return -1
  }
  if (start >= end) {
    return 1
  }

  start >>>= 0
  end >>>= 0
  thisStart >>>= 0
  thisEnd >>>= 0

  if (this === target) return 0

  var x = thisEnd - thisStart
  var y = end - start
  var len = Math.min(x, y)

  var thisCopy = this.slice(thisStart, thisEnd)
  var targetCopy = target.slice(start, end)

  for (var i = 0; i < len; ++i) {
    if (thisCopy[i] !== targetCopy[i]) {
      x = thisCopy[i]
      y = targetCopy[i]
      break
    }
  }

  if (x < y) return -1
  if (y < x) return 1
  return 0
}

function arrayIndexOf (arr, val, byteOffset, encoding) {
  var indexSize = 1
  var arrLength = arr.length
  var valLength = val.length

  if (encoding !== undefined) {
    encoding = String(encoding).toLowerCase()
    if (encoding === 'ucs2' || encoding === 'ucs-2' ||
        encoding === 'utf16le' || encoding === 'utf-16le') {
      if (arr.length < 2 || val.length < 2) {
        return -1
      }
      indexSize = 2
      arrLength /= 2
      valLength /= 2
      byteOffset /= 2
    }
  }

  function read (buf, i) {
    if (indexSize === 1) {
      return buf[i]
    } else {
      return buf.readUInt16BE(i * indexSize)
    }
  }

  var foundIndex = -1
  for (var i = byteOffset; i < arrLength; ++i) {
    if (read(arr, i) === read(val, foundIndex === -1 ? 0 : i - foundIndex)) {
      if (foundIndex === -1) foundIndex = i
      if (i - foundIndex + 1 === valLength) return foundIndex * indexSize
    } else {
      if (foundIndex !== -1) i -= i - foundIndex
      foundIndex = -1
    }
  }

  return -1
}

Buffer.prototype.indexOf = function indexOf (val, byteOffset, encoding) {
  if (typeof byteOffset === 'string') {
    encoding = byteOffset
    byteOffset = 0
  } else if (byteOffset > 0x7fffffff) {
    byteOffset = 0x7fffffff
  } else if (byteOffset < -0x80000000) {
    byteOffset = -0x80000000
  }
  byteOffset >>= 0

  if (this.length === 0) return -1
  if (byteOffset >= this.length) return -1

  // Negative offsets start from the end of the buffer
  if (byteOffset < 0) byteOffset = Math.max(this.length + byteOffset, 0)

  if (typeof val === 'string') {
    val = Buffer.from(val, encoding)
  }

  if (Buffer.isBuffer(val)) {
    // special case: looking for empty string/buffer always fails
    if (val.length === 0) {
      return -1
    }
    return arrayIndexOf(this, val, byteOffset, encoding)
  }
  if (typeof val === 'number') {
    if (Buffer.TYPED_ARRAY_SUPPORT && Uint8Array.prototype.indexOf === 'function') {
      return Uint8Array.prototype.indexOf.call(this, val, byteOffset)
    }
    return arrayIndexOf(this, [ val ], byteOffset, encoding)
  }

  throw new TypeError('val must be string, number or Buffer')
}

Buffer.prototype.includes = function includes (val, byteOffset, encoding) {
  return this.indexOf(val, byteOffset, encoding) !== -1
}

function hexWrite (buf, string, offset, length) {
  offset = Number(offset) || 0
  var remaining = buf.length - offset
  if (!length) {
    length = remaining
  } else {
    length = Number(length)
    if (length > remaining) {
      length = remaining
    }
  }

  // must be an even number of digits
  var strLen = string.length
  if (strLen % 2 !== 0) throw new Error('Invalid hex string')

  if (length > strLen / 2) {
    length = strLen / 2
  }
  for (var i = 0; i < length; ++i) {
    var parsed = parseInt(string.substr(i * 2, 2), 16)
    if (isNaN(parsed)) return i
    buf[offset + i] = parsed
  }
  return i
}

function utf8Write (buf, string, offset, length) {
  return blitBuffer(utf8ToBytes(string, buf.length - offset), buf, offset, length)
}

function asciiWrite (buf, string, offset, length) {
  return blitBuffer(asciiToBytes(string), buf, offset, length)
}

function binaryWrite (buf, string, offset, length) {
  return asciiWrite(buf, string, offset, length)
}

function base64Write (buf, string, offset, length) {
  return blitBuffer(base64ToBytes(string), buf, offset, length)
}

function ucs2Write (buf, string, offset, length) {
  return blitBuffer(utf16leToBytes(string, buf.length - offset), buf, offset, length)
}

Buffer.prototype.write = function write (string, offset, length, encoding) {
  // Buffer#write(string)
  if (offset === undefined) {
    encoding = 'utf8'
    length = this.length
    offset = 0
  // Buffer#write(string, encoding)
  } else if (length === undefined && typeof offset === 'string') {
    encoding = offset
    length = this.length
    offset = 0
  // Buffer#write(string, offset[, length][, encoding])
  } else if (isFinite(offset)) {
    offset = offset | 0
    if (isFinite(length)) {
      length = length | 0
      if (encoding === undefined) encoding = 'utf8'
    } else {
      encoding = length
      length = undefined
    }
  // legacy write(string, encoding, offset, length) - remove in v0.13
  } else {
    throw new Error(
      'Buffer.write(string, encoding, offset[, length]) is no longer supported'
    )
  }

  var remaining = this.length - offset
  if (length === undefined || length > remaining) length = remaining

  if ((string.length > 0 && (length < 0 || offset < 0)) || offset > this.length) {
    throw new RangeError('Attempt to write outside buffer bounds')
  }

  if (!encoding) encoding = 'utf8'

  var loweredCase = false
  for (;;) {
    switch (encoding) {
      case 'hex':
        return hexWrite(this, string, offset, length)

      case 'utf8':
      case 'utf-8':
        return utf8Write(this, string, offset, length)

      case 'ascii':
        return asciiWrite(this, string, offset, length)

      case 'binary':
        return binaryWrite(this, string, offset, length)

      case 'base64':
        // Warning: maxLength not taken into account in base64Write
        return base64Write(this, string, offset, length)

      case 'ucs2':
      case 'ucs-2':
      case 'utf16le':
      case 'utf-16le':
        return ucs2Write(this, string, offset, length)

      default:
        if (loweredCase) throw new TypeError('Unknown encoding: ' + encoding)
        encoding = ('' + encoding).toLowerCase()
        loweredCase = true
    }
  }
}

Buffer.prototype.toJSON = function toJSON () {
  return {
    type: 'Buffer',
    data: Array.prototype.slice.call(this._arr || this, 0)
  }
}

function base64Slice (buf, start, end) {
  if (start === 0 && end === buf.length) {
    return base64.fromByteArray(buf)
  } else {
    return base64.fromByteArray(buf.slice(start, end))
  }
}

function utf8Slice (buf, start, end) {
  end = Math.min(buf.length, end)
  var res = []

  var i = start
  while (i < end) {
    var firstByte = buf[i]
    var codePoint = null
    var bytesPerSequence = (firstByte > 0xEF) ? 4
      : (firstByte > 0xDF) ? 3
      : (firstByte > 0xBF) ? 2
      : 1

    if (i + bytesPerSequence <= end) {
      var secondByte, thirdByte, fourthByte, tempCodePoint

      switch (bytesPerSequence) {
        case 1:
          if (firstByte < 0x80) {
            codePoint = firstByte
          }
          break
        case 2:
          secondByte = buf[i + 1]
          if ((secondByte & 0xC0) === 0x80) {
            tempCodePoint = (firstByte & 0x1F) << 0x6 | (secondByte & 0x3F)
            if (tempCodePoint > 0x7F) {
              codePoint = tempCodePoint
            }
          }
          break
        case 3:
          secondByte = buf[i + 1]
          thirdByte = buf[i + 2]
          if ((secondByte & 0xC0) === 0x80 && (thirdByte & 0xC0) === 0x80) {
            tempCodePoint = (firstByte & 0xF) << 0xC | (secondByte & 0x3F) << 0x6 | (thirdByte & 0x3F)
            if (tempCodePoint > 0x7FF && (tempCodePoint < 0xD800 || tempCodePoint > 0xDFFF)) {
              codePoint = tempCodePoint
            }
          }
          break
        case 4:
          secondByte = buf[i + 1]
          thirdByte = buf[i + 2]
          fourthByte = buf[i + 3]
          if ((secondByte & 0xC0) === 0x80 && (thirdByte & 0xC0) === 0x80 && (fourthByte & 0xC0) === 0x80) {
            tempCodePoint = (firstByte & 0xF) << 0x12 | (secondByte & 0x3F) << 0xC | (thirdByte & 0x3F) << 0x6 | (fourthByte & 0x3F)
            if (tempCodePoint > 0xFFFF && tempCodePoint < 0x110000) {
              codePoint = tempCodePoint
            }
          }
      }
    }

    if (codePoint === null) {
      // we did not generate a valid codePoint so insert a
      // replacement char (U+FFFD) and advance only 1 byte
      codePoint = 0xFFFD
      bytesPerSequence = 1
    } else if (codePoint > 0xFFFF) {
      // encode to utf16 (surrogate pair dance)
      codePoint -= 0x10000
      res.push(codePoint >>> 10 & 0x3FF | 0xD800)
      codePoint = 0xDC00 | codePoint & 0x3FF
    }

    res.push(codePoint)
    i += bytesPerSequence
  }

  return decodeCodePointsArray(res)
}

// Based on http://stackoverflow.com/a/22747272/680742, the browser with
// the lowest limit is Chrome, with 0x10000 args.
// We go 1 magnitude less, for safety
var MAX_ARGUMENTS_LENGTH = 0x1000

function decodeCodePointsArray (codePoints) {
  var len = codePoints.length
  if (len <= MAX_ARGUMENTS_LENGTH) {
    return String.fromCharCode.apply(String, codePoints) // avoid extra slice()
  }

  // Decode in chunks to avoid "call stack size exceeded".
  var res = ''
  var i = 0
  while (i < len) {
    res += String.fromCharCode.apply(
      String,
      codePoints.slice(i, i += MAX_ARGUMENTS_LENGTH)
    )
  }
  return res
}

function asciiSlice (buf, start, end) {
  var ret = ''
  end = Math.min(buf.length, end)

  for (var i = start; i < end; ++i) {
    ret += String.fromCharCode(buf[i] & 0x7F)
  }
  return ret
}

function binarySlice (buf, start, end) {
  var ret = ''
  end = Math.min(buf.length, end)

  for (var i = start; i < end; ++i) {
    ret += String.fromCharCode(buf[i])
  }
  return ret
}

function hexSlice (buf, start, end) {
  var len = buf.length

  if (!start || start < 0) start = 0
  if (!end || end < 0 || end > len) end = len

  var out = ''
  for (var i = start; i < end; ++i) {
    out += toHex(buf[i])
  }
  return out
}

function utf16leSlice (buf, start, end) {
  var bytes = buf.slice(start, end)
  var res = ''
  for (var i = 0; i < bytes.length; i += 2) {
    res += String.fromCharCode(bytes[i] + bytes[i + 1] * 256)
  }
  return res
}

Buffer.prototype.slice = function slice (start, end) {
  var len = this.length
  start = ~~start
  end = end === undefined ? len : ~~end

  if (start < 0) {
    start += len
    if (start < 0) start = 0
  } else if (start > len) {
    start = len
  }

  if (end < 0) {
    end += len
    if (end < 0) end = 0
  } else if (end > len) {
    end = len
  }

  if (end < start) end = start

  var newBuf
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    newBuf = this.subarray(start, end)
    newBuf.__proto__ = Buffer.prototype
  } else {
    var sliceLen = end - start
    newBuf = new Buffer(sliceLen, undefined)
    for (var i = 0; i < sliceLen; ++i) {
      newBuf[i] = this[i + start]
    }
  }

  return newBuf
}

/*
 * Need to make sure that buffer isn't trying to write out of bounds.
 */
function checkOffset (offset, ext, length) {
  if ((offset % 1) !== 0 || offset < 0) throw new RangeError('offset is not uint')
  if (offset + ext > length) throw new RangeError('Trying to access beyond buffer length')
}

Buffer.prototype.readUIntLE = function readUIntLE (offset, byteLength, noAssert) {
  offset = offset | 0
  byteLength = byteLength | 0
  if (!noAssert) checkOffset(offset, byteLength, this.length)

  var val = this[offset]
  var mul = 1
  var i = 0
  while (++i < byteLength && (mul *= 0x100)) {
    val += this[offset + i] * mul
  }

  return val
}

Buffer.prototype.readUIntBE = function readUIntBE (offset, byteLength, noAssert) {
  offset = offset | 0
  byteLength = byteLength | 0
  if (!noAssert) {
    checkOffset(offset, byteLength, this.length)
  }

  var val = this[offset + --byteLength]
  var mul = 1
  while (byteLength > 0 && (mul *= 0x100)) {
    val += this[offset + --byteLength] * mul
  }

  return val
}

Buffer.prototype.readUInt8 = function readUInt8 (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 1, this.length)
  return this[offset]
}

Buffer.prototype.readUInt16LE = function readUInt16LE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 2, this.length)
  return this[offset] | (this[offset + 1] << 8)
}

Buffer.prototype.readUInt16BE = function readUInt16BE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 2, this.length)
  return (this[offset] << 8) | this[offset + 1]
}

Buffer.prototype.readUInt32LE = function readUInt32LE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 4, this.length)

  return ((this[offset]) |
      (this[offset + 1] << 8) |
      (this[offset + 2] << 16)) +
      (this[offset + 3] * 0x1000000)
}

Buffer.prototype.readUInt32BE = function readUInt32BE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 4, this.length)

  return (this[offset] * 0x1000000) +
    ((this[offset + 1] << 16) |
    (this[offset + 2] << 8) |
    this[offset + 3])
}

Buffer.prototype.readIntLE = function readIntLE (offset, byteLength, noAssert) {
  offset = offset | 0
  byteLength = byteLength | 0
  if (!noAssert) checkOffset(offset, byteLength, this.length)

  var val = this[offset]
  var mul = 1
  var i = 0
  while (++i < byteLength && (mul *= 0x100)) {
    val += this[offset + i] * mul
  }
  mul *= 0x80

  if (val >= mul) val -= Math.pow(2, 8 * byteLength)

  return val
}

Buffer.prototype.readIntBE = function readIntBE (offset, byteLength, noAssert) {
  offset = offset | 0
  byteLength = byteLength | 0
  if (!noAssert) checkOffset(offset, byteLength, this.length)

  var i = byteLength
  var mul = 1
  var val = this[offset + --i]
  while (i > 0 && (mul *= 0x100)) {
    val += this[offset + --i] * mul
  }
  mul *= 0x80

  if (val >= mul) val -= Math.pow(2, 8 * byteLength)

  return val
}

Buffer.prototype.readInt8 = function readInt8 (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 1, this.length)
  if (!(this[offset] & 0x80)) return (this[offset])
  return ((0xff - this[offset] + 1) * -1)
}

Buffer.prototype.readInt16LE = function readInt16LE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 2, this.length)
  var val = this[offset] | (this[offset + 1] << 8)
  return (val & 0x8000) ? val | 0xFFFF0000 : val
}

Buffer.prototype.readInt16BE = function readInt16BE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 2, this.length)
  var val = this[offset + 1] | (this[offset] << 8)
  return (val & 0x8000) ? val | 0xFFFF0000 : val
}

Buffer.prototype.readInt32LE = function readInt32LE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 4, this.length)

  return (this[offset]) |
    (this[offset + 1] << 8) |
    (this[offset + 2] << 16) |
    (this[offset + 3] << 24)
}

Buffer.prototype.readInt32BE = function readInt32BE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 4, this.length)

  return (this[offset] << 24) |
    (this[offset + 1] << 16) |
    (this[offset + 2] << 8) |
    (this[offset + 3])
}

Buffer.prototype.readFloatLE = function readFloatLE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 4, this.length)
  return ieee754.read(this, offset, true, 23, 4)
}

Buffer.prototype.readFloatBE = function readFloatBE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 4, this.length)
  return ieee754.read(this, offset, false, 23, 4)
}

Buffer.prototype.readDoubleLE = function readDoubleLE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 8, this.length)
  return ieee754.read(this, offset, true, 52, 8)
}

Buffer.prototype.readDoubleBE = function readDoubleBE (offset, noAssert) {
  if (!noAssert) checkOffset(offset, 8, this.length)
  return ieee754.read(this, offset, false, 52, 8)
}

function checkInt (buf, value, offset, ext, max, min) {
  if (!Buffer.isBuffer(buf)) throw new TypeError('"buffer" argument must be a Buffer instance')
  if (value > max || value < min) throw new RangeError('"value" argument is out of bounds')
  if (offset + ext > buf.length) throw new RangeError('Index out of range')
}

Buffer.prototype.writeUIntLE = function writeUIntLE (value, offset, byteLength, noAssert) {
  value = +value
  offset = offset | 0
  byteLength = byteLength | 0
  if (!noAssert) {
    var maxBytes = Math.pow(2, 8 * byteLength) - 1
    checkInt(this, value, offset, byteLength, maxBytes, 0)
  }

  var mul = 1
  var i = 0
  this[offset] = value & 0xFF
  while (++i < byteLength && (mul *= 0x100)) {
    this[offset + i] = (value / mul) & 0xFF
  }

  return offset + byteLength
}

Buffer.prototype.writeUIntBE = function writeUIntBE (value, offset, byteLength, noAssert) {
  value = +value
  offset = offset | 0
  byteLength = byteLength | 0
  if (!noAssert) {
    var maxBytes = Math.pow(2, 8 * byteLength) - 1
    checkInt(this, value, offset, byteLength, maxBytes, 0)
  }

  var i = byteLength - 1
  var mul = 1
  this[offset + i] = value & 0xFF
  while (--i >= 0 && (mul *= 0x100)) {
    this[offset + i] = (value / mul) & 0xFF
  }

  return offset + byteLength
}

Buffer.prototype.writeUInt8 = function writeUInt8 (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 1, 0xff, 0)
  if (!Buffer.TYPED_ARRAY_SUPPORT) value = Math.floor(value)
  this[offset] = (value & 0xff)
  return offset + 1
}

function objectWriteUInt16 (buf, value, offset, littleEndian) {
  if (value < 0) value = 0xffff + value + 1
  for (var i = 0, j = Math.min(buf.length - offset, 2); i < j; ++i) {
    buf[offset + i] = (value & (0xff << (8 * (littleEndian ? i : 1 - i)))) >>>
      (littleEndian ? i : 1 - i) * 8
  }
}

Buffer.prototype.writeUInt16LE = function writeUInt16LE (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 2, 0xffff, 0)
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    this[offset] = (value & 0xff)
    this[offset + 1] = (value >>> 8)
  } else {
    objectWriteUInt16(this, value, offset, true)
  }
  return offset + 2
}

Buffer.prototype.writeUInt16BE = function writeUInt16BE (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 2, 0xffff, 0)
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    this[offset] = (value >>> 8)
    this[offset + 1] = (value & 0xff)
  } else {
    objectWriteUInt16(this, value, offset, false)
  }
  return offset + 2
}

function objectWriteUInt32 (buf, value, offset, littleEndian) {
  if (value < 0) value = 0xffffffff + value + 1
  for (var i = 0, j = Math.min(buf.length - offset, 4); i < j; ++i) {
    buf[offset + i] = (value >>> (littleEndian ? i : 3 - i) * 8) & 0xff
  }
}

Buffer.prototype.writeUInt32LE = function writeUInt32LE (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 4, 0xffffffff, 0)
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    this[offset + 3] = (value >>> 24)
    this[offset + 2] = (value >>> 16)
    this[offset + 1] = (value >>> 8)
    this[offset] = (value & 0xff)
  } else {
    objectWriteUInt32(this, value, offset, true)
  }
  return offset + 4
}

Buffer.prototype.writeUInt32BE = function writeUInt32BE (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 4, 0xffffffff, 0)
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    this[offset] = (value >>> 24)
    this[offset + 1] = (value >>> 16)
    this[offset + 2] = (value >>> 8)
    this[offset + 3] = (value & 0xff)
  } else {
    objectWriteUInt32(this, value, offset, false)
  }
  return offset + 4
}

Buffer.prototype.writeIntLE = function writeIntLE (value, offset, byteLength, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) {
    var limit = Math.pow(2, 8 * byteLength - 1)

    checkInt(this, value, offset, byteLength, limit - 1, -limit)
  }

  var i = 0
  var mul = 1
  var sub = 0
  this[offset] = value & 0xFF
  while (++i < byteLength && (mul *= 0x100)) {
    if (value < 0 && sub === 0 && this[offset + i - 1] !== 0) {
      sub = 1
    }
    this[offset + i] = ((value / mul) >> 0) - sub & 0xFF
  }

  return offset + byteLength
}

Buffer.prototype.writeIntBE = function writeIntBE (value, offset, byteLength, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) {
    var limit = Math.pow(2, 8 * byteLength - 1)

    checkInt(this, value, offset, byteLength, limit - 1, -limit)
  }

  var i = byteLength - 1
  var mul = 1
  var sub = 0
  this[offset + i] = value & 0xFF
  while (--i >= 0 && (mul *= 0x100)) {
    if (value < 0 && sub === 0 && this[offset + i + 1] !== 0) {
      sub = 1
    }
    this[offset + i] = ((value / mul) >> 0) - sub & 0xFF
  }

  return offset + byteLength
}

Buffer.prototype.writeInt8 = function writeInt8 (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 1, 0x7f, -0x80)
  if (!Buffer.TYPED_ARRAY_SUPPORT) value = Math.floor(value)
  if (value < 0) value = 0xff + value + 1
  this[offset] = (value & 0xff)
  return offset + 1
}

Buffer.prototype.writeInt16LE = function writeInt16LE (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 2, 0x7fff, -0x8000)
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    this[offset] = (value & 0xff)
    this[offset + 1] = (value >>> 8)
  } else {
    objectWriteUInt16(this, value, offset, true)
  }
  return offset + 2
}

Buffer.prototype.writeInt16BE = function writeInt16BE (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 2, 0x7fff, -0x8000)
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    this[offset] = (value >>> 8)
    this[offset + 1] = (value & 0xff)
  } else {
    objectWriteUInt16(this, value, offset, false)
  }
  return offset + 2
}

Buffer.prototype.writeInt32LE = function writeInt32LE (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 4, 0x7fffffff, -0x80000000)
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    this[offset] = (value & 0xff)
    this[offset + 1] = (value >>> 8)
    this[offset + 2] = (value >>> 16)
    this[offset + 3] = (value >>> 24)
  } else {
    objectWriteUInt32(this, value, offset, true)
  }
  return offset + 4
}

Buffer.prototype.writeInt32BE = function writeInt32BE (value, offset, noAssert) {
  value = +value
  offset = offset | 0
  if (!noAssert) checkInt(this, value, offset, 4, 0x7fffffff, -0x80000000)
  if (value < 0) value = 0xffffffff + value + 1
  if (Buffer.TYPED_ARRAY_SUPPORT) {
    this[offset] = (value >>> 24)
    this[offset + 1] = (value >>> 16)
    this[offset + 2] = (value >>> 8)
    this[offset + 3] = (value & 0xff)
  } else {
    objectWriteUInt32(this, value, offset, false)
  }
  return offset + 4
}

function checkIEEE754 (buf, value, offset, ext, max, min) {
  if (offset + ext > buf.length) throw new RangeError('Index out of range')
  if (offset < 0) throw new RangeError('Index out of range')
}

function writeFloat (buf, value, offset, littleEndian, noAssert) {
  if (!noAssert) {
    checkIEEE754(buf, value, offset, 4, 3.4028234663852886e+38, -3.4028234663852886e+38)
  }
  ieee754.write(buf, value, offset, littleEndian, 23, 4)
  return offset + 4
}

Buffer.prototype.writeFloatLE = function writeFloatLE (value, offset, noAssert) {
  return writeFloat(this, value, offset, true, noAssert)
}

Buffer.prototype.writeFloatBE = function writeFloatBE (value, offset, noAssert) {
  return writeFloat(this, value, offset, false, noAssert)
}

function writeDouble (buf, value, offset, littleEndian, noAssert) {
  if (!noAssert) {
    checkIEEE754(buf, value, offset, 8, 1.7976931348623157E+308, -1.7976931348623157E+308)
  }
  ieee754.write(buf, value, offset, littleEndian, 52, 8)
  return offset + 8
}

Buffer.prototype.writeDoubleLE = function writeDoubleLE (value, offset, noAssert) {
  return writeDouble(this, value, offset, true, noAssert)
}

Buffer.prototype.writeDoubleBE = function writeDoubleBE (value, offset, noAssert) {
  return writeDouble(this, value, offset, false, noAssert)
}

// copy(targetBuffer, targetStart=0, sourceStart=0, sourceEnd=buffer.length)
Buffer.prototype.copy = function copy (target, targetStart, start, end) {
  if (!start) start = 0
  if (!end && end !== 0) end = this.length
  if (targetStart >= target.length) targetStart = target.length
  if (!targetStart) targetStart = 0
  if (end > 0 && end < start) end = start

  // Copy 0 bytes; we're done
  if (end === start) return 0
  if (target.length === 0 || this.length === 0) return 0

  // Fatal error conditions
  if (targetStart < 0) {
    throw new RangeError('targetStart out of bounds')
  }
  if (start < 0 || start >= this.length) throw new RangeError('sourceStart out of bounds')
  if (end < 0) throw new RangeError('sourceEnd out of bounds')

  // Are we oob?
  if (end > this.length) end = this.length
  if (target.length - targetStart < end - start) {
    end = target.length - targetStart + start
  }

  var len = end - start
  var i

  if (this === target && start < targetStart && targetStart < end) {
    // descending copy from end
    for (i = len - 1; i >= 0; --i) {
      target[i + targetStart] = this[i + start]
    }
  } else if (len < 1000 || !Buffer.TYPED_ARRAY_SUPPORT) {
    // ascending copy from start
    for (i = 0; i < len; ++i) {
      target[i + targetStart] = this[i + start]
    }
  } else {
    Uint8Array.prototype.set.call(
      target,
      this.subarray(start, start + len),
      targetStart
    )
  }

  return len
}

// Usage:
//    buffer.fill(number[, offset[, end]])
//    buffer.fill(buffer[, offset[, end]])
//    buffer.fill(string[, offset[, end]][, encoding])
Buffer.prototype.fill = function fill (val, start, end, encoding) {
  // Handle string cases:
  if (typeof val === 'string') {
    if (typeof start === 'string') {
      encoding = start
      start = 0
      end = this.length
    } else if (typeof end === 'string') {
      encoding = end
      end = this.length
    }
    if (val.length === 1) {
      var code = val.charCodeAt(0)
      if (code < 256) {
        val = code
      }
    }
    if (encoding !== undefined && typeof encoding !== 'string') {
      throw new TypeError('encoding must be a string')
    }
    if (typeof encoding === 'string' && !Buffer.isEncoding(encoding)) {
      throw new TypeError('Unknown encoding: ' + encoding)
    }
  } else if (typeof val === 'number') {
    val = val & 255
  }

  // Invalid ranges are not set to a default, so can range check early.
  if (start < 0 || this.length < start || this.length < end) {
    throw new RangeError('Out of range index')
  }

  if (end <= start) {
    return this
  }

  start = start >>> 0
  end = end === undefined ? this.length : end >>> 0

  if (!val) val = 0

  var i
  if (typeof val === 'number') {
    for (i = start; i < end; ++i) {
      this[i] = val
    }
  } else {
    var bytes = Buffer.isBuffer(val)
      ? val
      : utf8ToBytes(new Buffer(val, encoding).toString())
    var len = bytes.length
    for (i = 0; i < end - start; ++i) {
      this[i + start] = bytes[i % len]
    }
  }

  return this
}

// HELPER FUNCTIONS
// ================

var INVALID_BASE64_RE = /[^+\/0-9A-Za-z-_]/g

function base64clean (str) {
  // Node strips out invalid characters like \n and \t from the string, base64-js does not
  str = stringtrim(str).replace(INVALID_BASE64_RE, '')
  // Node converts strings with length < 2 to ''
  if (str.length < 2) return ''
  // Node allows for non-padded base64 strings (missing trailing ===), base64-js does not
  while (str.length % 4 !== 0) {
    str = str + '='
  }
  return str
}

function stringtrim (str) {
  if (str.trim) return str.trim()
  return str.replace(/^\s+|\s+$/g, '')
}

function toHex (n) {
  if (n < 16) return '0' + n.toString(16)
  return n.toString(16)
}

function utf8ToBytes (string, units) {
  units = units || Infinity
  var codePoint
  var length = string.length
  var leadSurrogate = null
  var bytes = []

  for (var i = 0; i < length; ++i) {
    codePoint = string.charCodeAt(i)

    // is surrogate component
    if (codePoint > 0xD7FF && codePoint < 0xE000) {
      // last char was a lead
      if (!leadSurrogate) {
        // no lead yet
        if (codePoint > 0xDBFF) {
          // unexpected trail
          if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
          continue
        } else if (i + 1 === length) {
          // unpaired lead
          if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
          continue
        }

        // valid lead
        leadSurrogate = codePoint

        continue
      }

      // 2 leads in a row
      if (codePoint < 0xDC00) {
        if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
        leadSurrogate = codePoint
        continue
      }

      // valid surrogate pair
      codePoint = (leadSurrogate - 0xD800 << 10 | codePoint - 0xDC00) + 0x10000
    } else if (leadSurrogate) {
      // valid bmp char, but last char was a lead
      if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
    }

    leadSurrogate = null

    // encode utf8
    if (codePoint < 0x80) {
      if ((units -= 1) < 0) break
      bytes.push(codePoint)
    } else if (codePoint < 0x800) {
      if ((units -= 2) < 0) break
      bytes.push(
        codePoint >> 0x6 | 0xC0,
        codePoint & 0x3F | 0x80
      )
    } else if (codePoint < 0x10000) {
      if ((units -= 3) < 0) break
      bytes.push(
        codePoint >> 0xC | 0xE0,
        codePoint >> 0x6 & 0x3F | 0x80,
        codePoint & 0x3F | 0x80
      )
    } else if (codePoint < 0x110000) {
      if ((units -= 4) < 0) break
      bytes.push(
        codePoint >> 0x12 | 0xF0,
        codePoint >> 0xC & 0x3F | 0x80,
        codePoint >> 0x6 & 0x3F | 0x80,
        codePoint & 0x3F | 0x80
      )
    } else {
      throw new Error('Invalid code point')
    }
  }

  return bytes
}

function asciiToBytes (str) {
  var byteArray = []
  for (var i = 0; i < str.length; ++i) {
    // Node's code seems to be doing this and not & 0x7F..
    byteArray.push(str.charCodeAt(i) & 0xFF)
  }
  return byteArray
}

function utf16leToBytes (str, units) {
  var c, hi, lo
  var byteArray = []
  for (var i = 0; i < str.length; ++i) {
    if ((units -= 2) < 0) break

    c = str.charCodeAt(i)
    hi = c >> 8
    lo = c % 256
    byteArray.push(lo)
    byteArray.push(hi)
  }

  return byteArray
}

function base64ToBytes (str) {
  return base64.toByteArray(base64clean(str))
}

function blitBuffer (src, dst, offset, length) {
  for (var i = 0; i < length; ++i) {
    if ((i + offset >= dst.length) || (i >= src.length)) break
    dst[i + offset] = src[i]
  }
  return i
}

function isnan (val) {
  return val !== val // eslint-disable-line no-self-compare
}

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{"base64-js":1,"ieee754":7,"isarray":10}],5:[function(require,module,exports){
(function (Buffer){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

// NOTE: These type checking functions intentionally don't use `instanceof`
// because it is fragile and can be easily faked with `Object.create()`.

function isArray(arg) {
  if (Array.isArray) {
    return Array.isArray(arg);
  }
  return objectToString(arg) === '[object Array]';
}
exports.isArray = isArray;

function isBoolean(arg) {
  return typeof arg === 'boolean';
}
exports.isBoolean = isBoolean;

function isNull(arg) {
  return arg === null;
}
exports.isNull = isNull;

function isNullOrUndefined(arg) {
  return arg == null;
}
exports.isNullOrUndefined = isNullOrUndefined;

function isNumber(arg) {
  return typeof arg === 'number';
}
exports.isNumber = isNumber;

function isString(arg) {
  return typeof arg === 'string';
}
exports.isString = isString;

function isSymbol(arg) {
  return typeof arg === 'symbol';
}
exports.isSymbol = isSymbol;

function isUndefined(arg) {
  return arg === void 0;
}
exports.isUndefined = isUndefined;

function isRegExp(re) {
  return objectToString(re) === '[object RegExp]';
}
exports.isRegExp = isRegExp;

function isObject(arg) {
  return typeof arg === 'object' && arg !== null;
}
exports.isObject = isObject;

function isDate(d) {
  return objectToString(d) === '[object Date]';
}
exports.isDate = isDate;

function isError(e) {
  return (objectToString(e) === '[object Error]' || e instanceof Error);
}
exports.isError = isError;

function isFunction(arg) {
  return typeof arg === 'function';
}
exports.isFunction = isFunction;

function isPrimitive(arg) {
  return arg === null ||
         typeof arg === 'boolean' ||
         typeof arg === 'number' ||
         typeof arg === 'string' ||
         typeof arg === 'symbol' ||  // ES6 symbol
         typeof arg === 'undefined';
}
exports.isPrimitive = isPrimitive;

exports.isBuffer = Buffer.isBuffer;

function objectToString(o) {
  return Object.prototype.toString.call(o);
}

}).call(this,{"isBuffer":require("../../is-buffer/index.js")})
},{"../../is-buffer/index.js":9}],6:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

function EventEmitter() {
  this._events = this._events || {};
  this._maxListeners = this._maxListeners || undefined;
}
module.exports = EventEmitter;

// Backwards-compat with node 0.10.x
EventEmitter.EventEmitter = EventEmitter;

EventEmitter.prototype._events = undefined;
EventEmitter.prototype._maxListeners = undefined;

// By default EventEmitters will print a warning if more than 10 listeners are
// added to it. This is a useful default which helps finding memory leaks.
EventEmitter.defaultMaxListeners = 10;

// Obviously not all Emitters should be limited to 10. This function allows
// that to be increased. Set to zero for unlimited.
EventEmitter.prototype.setMaxListeners = function(n) {
  if (!isNumber(n) || n < 0 || isNaN(n))
    throw TypeError('n must be a positive number');
  this._maxListeners = n;
  return this;
};

EventEmitter.prototype.emit = function(type) {
  var er, handler, len, args, i, listeners;

  if (!this._events)
    this._events = {};

  // If there is no 'error' event listener then throw.
  if (type === 'error') {
    if (!this._events.error ||
        (isObject(this._events.error) && !this._events.error.length)) {
      er = arguments[1];
      if (er instanceof Error) {
        throw er; // Unhandled 'error' event
      } else {
        // At least give some kind of context to the user
        var err = new Error('Uncaught, unspecified "error" event. (' + er + ')');
        err.context = er;
        throw err;
      }
    }
  }

  handler = this._events[type];

  if (isUndefined(handler))
    return false;

  if (isFunction(handler)) {
    switch (arguments.length) {
      // fast cases
      case 1:
        handler.call(this);
        break;
      case 2:
        handler.call(this, arguments[1]);
        break;
      case 3:
        handler.call(this, arguments[1], arguments[2]);
        break;
      // slower
      default:
        args = Array.prototype.slice.call(arguments, 1);
        handler.apply(this, args);
    }
  } else if (isObject(handler)) {
    args = Array.prototype.slice.call(arguments, 1);
    listeners = handler.slice();
    len = listeners.length;
    for (i = 0; i < len; i++)
      listeners[i].apply(this, args);
  }

  return true;
};

EventEmitter.prototype.addListener = function(type, listener) {
  var m;

  if (!isFunction(listener))
    throw TypeError('listener must be a function');

  if (!this._events)
    this._events = {};

  // To avoid recursion in the case that type === "newListener"! Before
  // adding it to the listeners, first emit "newListener".
  if (this._events.newListener)
    this.emit('newListener', type,
              isFunction(listener.listener) ?
              listener.listener : listener);

  if (!this._events[type])
    // Optimize the case of one listener. Don't need the extra array object.
    this._events[type] = listener;
  else if (isObject(this._events[type]))
    // If we've already got an array, just append.
    this._events[type].push(listener);
  else
    // Adding the second element, need to change to array.
    this._events[type] = [this._events[type], listener];

  // Check for listener leak
  if (isObject(this._events[type]) && !this._events[type].warned) {
    if (!isUndefined(this._maxListeners)) {
      m = this._maxListeners;
    } else {
      m = EventEmitter.defaultMaxListeners;
    }

    if (m && m > 0 && this._events[type].length > m) {
      this._events[type].warned = true;
      console.error('(node) warning: possible EventEmitter memory ' +
                    'leak detected. %d listeners added. ' +
                    'Use emitter.setMaxListeners() to increase limit.',
                    this._events[type].length);
      if (typeof console.trace === 'function') {
        // not supported in IE 10
        console.trace();
      }
    }
  }

  return this;
};

EventEmitter.prototype.on = EventEmitter.prototype.addListener;

EventEmitter.prototype.once = function(type, listener) {
  if (!isFunction(listener))
    throw TypeError('listener must be a function');

  var fired = false;

  function g() {
    this.removeListener(type, g);

    if (!fired) {
      fired = true;
      listener.apply(this, arguments);
    }
  }

  g.listener = listener;
  this.on(type, g);

  return this;
};

// emits a 'removeListener' event iff the listener was removed
EventEmitter.prototype.removeListener = function(type, listener) {
  var list, position, length, i;

  if (!isFunction(listener))
    throw TypeError('listener must be a function');

  if (!this._events || !this._events[type])
    return this;

  list = this._events[type];
  length = list.length;
  position = -1;

  if (list === listener ||
      (isFunction(list.listener) && list.listener === listener)) {
    delete this._events[type];
    if (this._events.removeListener)
      this.emit('removeListener', type, listener);

  } else if (isObject(list)) {
    for (i = length; i-- > 0;) {
      if (list[i] === listener ||
          (list[i].listener && list[i].listener === listener)) {
        position = i;
        break;
      }
    }

    if (position < 0)
      return this;

    if (list.length === 1) {
      list.length = 0;
      delete this._events[type];
    } else {
      list.splice(position, 1);
    }

    if (this._events.removeListener)
      this.emit('removeListener', type, listener);
  }

  return this;
};

EventEmitter.prototype.removeAllListeners = function(type) {
  var key, listeners;

  if (!this._events)
    return this;

  // not listening for removeListener, no need to emit
  if (!this._events.removeListener) {
    if (arguments.length === 0)
      this._events = {};
    else if (this._events[type])
      delete this._events[type];
    return this;
  }

  // emit removeListener for all listeners on all events
  if (arguments.length === 0) {
    for (key in this._events) {
      if (key === 'removeListener') continue;
      this.removeAllListeners(key);
    }
    this.removeAllListeners('removeListener');
    this._events = {};
    return this;
  }

  listeners = this._events[type];

  if (isFunction(listeners)) {
    this.removeListener(type, listeners);
  } else if (listeners) {
    // LIFO order
    while (listeners.length)
      this.removeListener(type, listeners[listeners.length - 1]);
  }
  delete this._events[type];

  return this;
};

EventEmitter.prototype.listeners = function(type) {
  var ret;
  if (!this._events || !this._events[type])
    ret = [];
  else if (isFunction(this._events[type]))
    ret = [this._events[type]];
  else
    ret = this._events[type].slice();
  return ret;
};

EventEmitter.prototype.listenerCount = function(type) {
  if (this._events) {
    var evlistener = this._events[type];

    if (isFunction(evlistener))
      return 1;
    else if (evlistener)
      return evlistener.length;
  }
  return 0;
};

EventEmitter.listenerCount = function(emitter, type) {
  return emitter.listenerCount(type);
};

function isFunction(arg) {
  return typeof arg === 'function';
}

function isNumber(arg) {
  return typeof arg === 'number';
}

function isObject(arg) {
  return typeof arg === 'object' && arg !== null;
}

function isUndefined(arg) {
  return arg === void 0;
}

},{}],7:[function(require,module,exports){
exports.read = function (buffer, offset, isLE, mLen, nBytes) {
  var e, m
  var eLen = nBytes * 8 - mLen - 1
  var eMax = (1 << eLen) - 1
  var eBias = eMax >> 1
  var nBits = -7
  var i = isLE ? (nBytes - 1) : 0
  var d = isLE ? -1 : 1
  var s = buffer[offset + i]

  i += d

  e = s & ((1 << (-nBits)) - 1)
  s >>= (-nBits)
  nBits += eLen
  for (; nBits > 0; e = e * 256 + buffer[offset + i], i += d, nBits -= 8) {}

  m = e & ((1 << (-nBits)) - 1)
  e >>= (-nBits)
  nBits += mLen
  for (; nBits > 0; m = m * 256 + buffer[offset + i], i += d, nBits -= 8) {}

  if (e === 0) {
    e = 1 - eBias
  } else if (e === eMax) {
    return m ? NaN : ((s ? -1 : 1) * Infinity)
  } else {
    m = m + Math.pow(2, mLen)
    e = e - eBias
  }
  return (s ? -1 : 1) * m * Math.pow(2, e - mLen)
}

exports.write = function (buffer, value, offset, isLE, mLen, nBytes) {
  var e, m, c
  var eLen = nBytes * 8 - mLen - 1
  var eMax = (1 << eLen) - 1
  var eBias = eMax >> 1
  var rt = (mLen === 23 ? Math.pow(2, -24) - Math.pow(2, -77) : 0)
  var i = isLE ? 0 : (nBytes - 1)
  var d = isLE ? 1 : -1
  var s = value < 0 || (value === 0 && 1 / value < 0) ? 1 : 0

  value = Math.abs(value)

  if (isNaN(value) || value === Infinity) {
    m = isNaN(value) ? 1 : 0
    e = eMax
  } else {
    e = Math.floor(Math.log(value) / Math.LN2)
    if (value * (c = Math.pow(2, -e)) < 1) {
      e--
      c *= 2
    }
    if (e + eBias >= 1) {
      value += rt / c
    } else {
      value += rt * Math.pow(2, 1 - eBias)
    }
    if (value * c >= 2) {
      e++
      c /= 2
    }

    if (e + eBias >= eMax) {
      m = 0
      e = eMax
    } else if (e + eBias >= 1) {
      m = (value * c - 1) * Math.pow(2, mLen)
      e = e + eBias
    } else {
      m = value * Math.pow(2, eBias - 1) * Math.pow(2, mLen)
      e = 0
    }
  }

  for (; mLen >= 8; buffer[offset + i] = m & 0xff, i += d, m /= 256, mLen -= 8) {}

  e = (e << mLen) | m
  eLen += mLen
  for (; eLen > 0; buffer[offset + i] = e & 0xff, i += d, e /= 256, eLen -= 8) {}

  buffer[offset + i - d] |= s * 128
}

},{}],8:[function(require,module,exports){
if (typeof Object.create === 'function') {
  // implementation from standard node.js 'util' module
  module.exports = function inherits(ctor, superCtor) {
    ctor.super_ = superCtor
    ctor.prototype = Object.create(superCtor.prototype, {
      constructor: {
        value: ctor,
        enumerable: false,
        writable: true,
        configurable: true
      }
    });
  };
} else {
  // old school shim for old browsers
  module.exports = function inherits(ctor, superCtor) {
    ctor.super_ = superCtor
    var TempCtor = function () {}
    TempCtor.prototype = superCtor.prototype
    ctor.prototype = new TempCtor()
    ctor.prototype.constructor = ctor
  }
}

},{}],9:[function(require,module,exports){
/**
 * Determine if an object is Buffer
 *
 * Author:   Feross Aboukhadijeh <feross@feross.org> <http://feross.org>
 * License:  MIT
 *
 * `npm install is-buffer`
 */

module.exports = function (obj) {
  return !!(obj != null &&
    (obj._isBuffer || // For Safari 5-7 (missing Object.prototype.constructor)
      (obj.constructor &&
      typeof obj.constructor.isBuffer === 'function' &&
      obj.constructor.isBuffer(obj))
    ))
}

},{}],10:[function(require,module,exports){
var toString = {}.toString;

module.exports = Array.isArray || function (arr) {
  return toString.call(arr) == '[object Array]';
};

},{}],11:[function(require,module,exports){
(function (process){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

// resolves . and .. elements in a path array with directory names there
// must be no slashes, empty elements, or device names (c:\) in the array
// (so also no leading and trailing slashes - it does not distinguish
// relative and absolute paths)
function normalizeArray(parts, allowAboveRoot) {
  // if the path tries to go above the root, `up` ends up > 0
  var up = 0;
  for (var i = parts.length - 1; i >= 0; i--) {
    var last = parts[i];
    if (last === '.') {
      parts.splice(i, 1);
    } else if (last === '..') {
      parts.splice(i, 1);
      up++;
    } else if (up) {
      parts.splice(i, 1);
      up--;
    }
  }

  // if the path is allowed to go above the root, restore leading ..s
  if (allowAboveRoot) {
    for (; up--; up) {
      parts.unshift('..');
    }
  }

  return parts;
}

// Split a filename into [root, dir, basename, ext], unix version
// 'root' is just a slash, or nothing.
var splitPathRe =
    /^(\/?|)([\s\S]*?)((?:\.{1,2}|[^\/]+?|)(\.[^.\/]*|))(?:[\/]*)$/;
var splitPath = function(filename) {
  return splitPathRe.exec(filename).slice(1);
};

// path.resolve([from ...], to)
// posix version
exports.resolve = function() {
  var resolvedPath = '',
      resolvedAbsolute = false;

  for (var i = arguments.length - 1; i >= -1 && !resolvedAbsolute; i--) {
    var path = (i >= 0) ? arguments[i] : process.cwd();

    // Skip empty and invalid entries
    if (typeof path !== 'string') {
      throw new TypeError('Arguments to path.resolve must be strings');
    } else if (!path) {
      continue;
    }

    resolvedPath = path + '/' + resolvedPath;
    resolvedAbsolute = path.charAt(0) === '/';
  }

  // At this point the path should be resolved to a full absolute path, but
  // handle relative paths to be safe (might happen when process.cwd() fails)

  // Normalize the path
  resolvedPath = normalizeArray(filter(resolvedPath.split('/'), function(p) {
    return !!p;
  }), !resolvedAbsolute).join('/');

  return ((resolvedAbsolute ? '/' : '') + resolvedPath) || '.';
};

// path.normalize(path)
// posix version
exports.normalize = function(path) {
  var isAbsolute = exports.isAbsolute(path),
      trailingSlash = substr(path, -1) === '/';

  // Normalize the path
  path = normalizeArray(filter(path.split('/'), function(p) {
    return !!p;
  }), !isAbsolute).join('/');

  if (!path && !isAbsolute) {
    path = '.';
  }
  if (path && trailingSlash) {
    path += '/';
  }

  return (isAbsolute ? '/' : '') + path;
};

// posix version
exports.isAbsolute = function(path) {
  return path.charAt(0) === '/';
};

// posix version
exports.join = function() {
  var paths = Array.prototype.slice.call(arguments, 0);
  return exports.normalize(filter(paths, function(p, index) {
    if (typeof p !== 'string') {
      throw new TypeError('Arguments to path.join must be strings');
    }
    return p;
  }).join('/'));
};


// path.relative(from, to)
// posix version
exports.relative = function(from, to) {
  from = exports.resolve(from).substr(1);
  to = exports.resolve(to).substr(1);

  function trim(arr) {
    var start = 0;
    for (; start < arr.length; start++) {
      if (arr[start] !== '') break;
    }

    var end = arr.length - 1;
    for (; end >= 0; end--) {
      if (arr[end] !== '') break;
    }

    if (start > end) return [];
    return arr.slice(start, end - start + 1);
  }

  var fromParts = trim(from.split('/'));
  var toParts = trim(to.split('/'));

  var length = Math.min(fromParts.length, toParts.length);
  var samePartsLength = length;
  for (var i = 0; i < length; i++) {
    if (fromParts[i] !== toParts[i]) {
      samePartsLength = i;
      break;
    }
  }

  var outputParts = [];
  for (var i = samePartsLength; i < fromParts.length; i++) {
    outputParts.push('..');
  }

  outputParts = outputParts.concat(toParts.slice(samePartsLength));

  return outputParts.join('/');
};

exports.sep = '/';
exports.delimiter = ':';

exports.dirname = function(path) {
  var result = splitPath(path),
      root = result[0],
      dir = result[1];

  if (!root && !dir) {
    // No dirname whatsoever
    return '.';
  }

  if (dir) {
    // It has a dirname, strip trailing slash
    dir = dir.substr(0, dir.length - 1);
  }

  return root + dir;
};


exports.basename = function(path, ext) {
  var f = splitPath(path)[2];
  // TODO: make this comparison case-insensitive on windows?
  if (ext && f.substr(-1 * ext.length) === ext) {
    f = f.substr(0, f.length - ext.length);
  }
  return f;
};


exports.extname = function(path) {
  return splitPath(path)[3];
};

function filter (xs, f) {
    if (xs.filter) return xs.filter(f);
    var res = [];
    for (var i = 0; i < xs.length; i++) {
        if (f(xs[i], i, xs)) res.push(xs[i]);
    }
    return res;
}

// String.prototype.substr - negative index don't work in IE8
var substr = 'ab'.substr(-1) === 'b'
    ? function (str, start, len) { return str.substr(start, len) }
    : function (str, start, len) {
        if (start < 0) start = str.length + start;
        return str.substr(start, len);
    }
;

}).call(this,require('_process'))
},{"_process":13}],12:[function(require,module,exports){
(function (process){
'use strict';

if (!process.version ||
    process.version.indexOf('v0.') === 0 ||
    process.version.indexOf('v1.') === 0 && process.version.indexOf('v1.8.') !== 0) {
  module.exports = nextTick;
} else {
  module.exports = process.nextTick;
}

function nextTick(fn, arg1, arg2, arg3) {
  if (typeof fn !== 'function') {
    throw new TypeError('"callback" argument must be a function');
  }
  var len = arguments.length;
  var args, i;
  switch (len) {
  case 0:
  case 1:
    return process.nextTick(fn);
  case 2:
    return process.nextTick(function afterTickOne() {
      fn.call(null, arg1);
    });
  case 3:
    return process.nextTick(function afterTickTwo() {
      fn.call(null, arg1, arg2);
    });
  case 4:
    return process.nextTick(function afterTickThree() {
      fn.call(null, arg1, arg2, arg3);
    });
  default:
    args = new Array(len - 1);
    i = 0;
    while (i < args.length) {
      args[i++] = arguments[i];
    }
    return process.nextTick(function afterTick() {
      fn.apply(null, args);
    });
  }
}

}).call(this,require('_process'))
},{"_process":13}],13:[function(require,module,exports){
// shim for using process in browser

var process = module.exports = {};

// cached from whatever global is present so that test runners that stub it
// don't break things.  But we need to wrap it in a try catch in case it is
// wrapped in strict mode code which doesn't define any globals.  It's inside a
// function because try/catches deoptimize in certain engines.

var cachedSetTimeout;
var cachedClearTimeout;

(function () {
  try {
    cachedSetTimeout = setTimeout;
  } catch (e) {
    cachedSetTimeout = function () {
      throw new Error('setTimeout is not defined');
    }
  }
  try {
    cachedClearTimeout = clearTimeout;
  } catch (e) {
    cachedClearTimeout = function () {
      throw new Error('clearTimeout is not defined');
    }
  }
} ())
var queue = [];
var draining = false;
var currentQueue;
var queueIndex = -1;

function cleanUpNextTick() {
    if (!draining || !currentQueue) {
        return;
    }
    draining = false;
    if (currentQueue.length) {
        queue = currentQueue.concat(queue);
    } else {
        queueIndex = -1;
    }
    if (queue.length) {
        drainQueue();
    }
}

function drainQueue() {
    if (draining) {
        return;
    }
    var timeout = cachedSetTimeout(cleanUpNextTick);
    draining = true;

    var len = queue.length;
    while(len) {
        currentQueue = queue;
        queue = [];
        while (++queueIndex < len) {
            if (currentQueue) {
                currentQueue[queueIndex].run();
            }
        }
        queueIndex = -1;
        len = queue.length;
    }
    currentQueue = null;
    draining = false;
    cachedClearTimeout(timeout);
}

process.nextTick = function (fun) {
    var args = new Array(arguments.length - 1);
    if (arguments.length > 1) {
        for (var i = 1; i < arguments.length; i++) {
            args[i - 1] = arguments[i];
        }
    }
    queue.push(new Item(fun, args));
    if (queue.length === 1 && !draining) {
        cachedSetTimeout(drainQueue, 0);
    }
};

// v8 likes predictible objects
function Item(fun, array) {
    this.fun = fun;
    this.array = array;
}
Item.prototype.run = function () {
    this.fun.apply(null, this.array);
};
process.title = 'browser';
process.browser = true;
process.env = {};
process.argv = [];
process.version = ''; // empty string to avoid regexp issues
process.versions = {};

function noop() {}

process.on = noop;
process.addListener = noop;
process.once = noop;
process.off = noop;
process.removeListener = noop;
process.removeAllListeners = noop;
process.emit = noop;

process.binding = function (name) {
    throw new Error('process.binding is not supported');
};

process.cwd = function () { return '/' };
process.chdir = function (dir) {
    throw new Error('process.chdir is not supported');
};
process.umask = function() { return 0; };

},{}],14:[function(require,module,exports){
module.exports = require("./lib/_stream_duplex.js")

},{"./lib/_stream_duplex.js":15}],15:[function(require,module,exports){
// a duplex stream is just a stream that is both readable and writable.
// Since JS doesn't have multiple prototypal inheritance, this class
// prototypally inherits from Readable, and then parasitically from
// Writable.

'use strict';

/*<replacement>*/

var objectKeys = Object.keys || function (obj) {
  var keys = [];
  for (var key in obj) {
    keys.push(key);
  }return keys;
};
/*</replacement>*/

module.exports = Duplex;

/*<replacement>*/
var processNextTick = require('process-nextick-args');
/*</replacement>*/

/*<replacement>*/
var util = require('core-util-is');
util.inherits = require('inherits');
/*</replacement>*/

var Readable = require('./_stream_readable');
var Writable = require('./_stream_writable');

util.inherits(Duplex, Readable);

var keys = objectKeys(Writable.prototype);
for (var v = 0; v < keys.length; v++) {
  var method = keys[v];
  if (!Duplex.prototype[method]) Duplex.prototype[method] = Writable.prototype[method];
}

function Duplex(options) {
  if (!(this instanceof Duplex)) return new Duplex(options);

  Readable.call(this, options);
  Writable.call(this, options);

  if (options && options.readable === false) this.readable = false;

  if (options && options.writable === false) this.writable = false;

  this.allowHalfOpen = true;
  if (options && options.allowHalfOpen === false) this.allowHalfOpen = false;

  this.once('end', onend);
}

// the no-half-open enforcer
function onend() {
  // if we allow half-open state, or if the writable side ended,
  // then we're ok.
  if (this.allowHalfOpen || this._writableState.ended) return;

  // no more data can be written.
  // But allow more writes to happen in this tick.
  processNextTick(onEndNT, this);
}

function onEndNT(self) {
  self.end();
}

function forEach(xs, f) {
  for (var i = 0, l = xs.length; i < l; i++) {
    f(xs[i], i);
  }
}
},{"./_stream_readable":17,"./_stream_writable":19,"core-util-is":5,"inherits":8,"process-nextick-args":12}],16:[function(require,module,exports){
// a passthrough stream.
// basically just the most minimal sort of Transform stream.
// Every written chunk gets output as-is.

'use strict';

module.exports = PassThrough;

var Transform = require('./_stream_transform');

/*<replacement>*/
var util = require('core-util-is');
util.inherits = require('inherits');
/*</replacement>*/

util.inherits(PassThrough, Transform);

function PassThrough(options) {
  if (!(this instanceof PassThrough)) return new PassThrough(options);

  Transform.call(this, options);
}

PassThrough.prototype._transform = function (chunk, encoding, cb) {
  cb(null, chunk);
};
},{"./_stream_transform":18,"core-util-is":5,"inherits":8}],17:[function(require,module,exports){
(function (process){
'use strict';

module.exports = Readable;

/*<replacement>*/
var processNextTick = require('process-nextick-args');
/*</replacement>*/

/*<replacement>*/
var isArray = require('isarray');
/*</replacement>*/

Readable.ReadableState = ReadableState;

/*<replacement>*/
var EE = require('events').EventEmitter;

var EElistenerCount = function (emitter, type) {
  return emitter.listeners(type).length;
};
/*</replacement>*/

/*<replacement>*/
var Stream;
(function () {
  try {
    Stream = require('st' + 'ream');
  } catch (_) {} finally {
    if (!Stream) Stream = require('events').EventEmitter;
  }
})();
/*</replacement>*/

var Buffer = require('buffer').Buffer;
/*<replacement>*/
var bufferShim = require('buffer-shims');
/*</replacement>*/

/*<replacement>*/
var util = require('core-util-is');
util.inherits = require('inherits');
/*</replacement>*/

/*<replacement>*/
var debugUtil = require('util');
var debug = void 0;
if (debugUtil && debugUtil.debuglog) {
  debug = debugUtil.debuglog('stream');
} else {
  debug = function () {};
}
/*</replacement>*/

var StringDecoder;

util.inherits(Readable, Stream);

var hasPrependListener = typeof EE.prototype.prependListener === 'function';

function prependListener(emitter, event, fn) {
  if (hasPrependListener) return emitter.prependListener(event, fn);

  // This is a brutally ugly hack to make sure that our error handler
  // is attached before any userland ones.  NEVER DO THIS. This is here
  // only because this code needs to continue to work with older versions
  // of Node.js that do not include the prependListener() method. The goal
  // is to eventually remove this hack.
  if (!emitter._events || !emitter._events[event]) emitter.on(event, fn);else if (isArray(emitter._events[event])) emitter._events[event].unshift(fn);else emitter._events[event] = [fn, emitter._events[event]];
}

var Duplex;
function ReadableState(options, stream) {
  Duplex = Duplex || require('./_stream_duplex');

  options = options || {};

  // object stream flag. Used to make read(n) ignore n and to
  // make all the buffer merging and length checks go away
  this.objectMode = !!options.objectMode;

  if (stream instanceof Duplex) this.objectMode = this.objectMode || !!options.readableObjectMode;

  // the point at which it stops calling _read() to fill the buffer
  // Note: 0 is a valid value, means "don't call _read preemptively ever"
  var hwm = options.highWaterMark;
  var defaultHwm = this.objectMode ? 16 : 16 * 1024;
  this.highWaterMark = hwm || hwm === 0 ? hwm : defaultHwm;

  // cast to ints.
  this.highWaterMark = ~ ~this.highWaterMark;

  this.buffer = [];
  this.length = 0;
  this.pipes = null;
  this.pipesCount = 0;
  this.flowing = null;
  this.ended = false;
  this.endEmitted = false;
  this.reading = false;

  // a flag to be able to tell if the onwrite cb is called immediately,
  // or on a later tick.  We set this to true at first, because any
  // actions that shouldn't happen until "later" should generally also
  // not happen before the first write call.
  this.sync = true;

  // whenever we return null, then we set a flag to say
  // that we're awaiting a 'readable' event emission.
  this.needReadable = false;
  this.emittedReadable = false;
  this.readableListening = false;
  this.resumeScheduled = false;

  // Crypto is kind of old and crusty.  Historically, its default string
  // encoding is 'binary' so we have to make this configurable.
  // Everything else in the universe uses 'utf8', though.
  this.defaultEncoding = options.defaultEncoding || 'utf8';

  // when piping, we only care about 'readable' events that happen
  // after read()ing all the bytes and not getting any pushback.
  this.ranOut = false;

  // the number of writers that are awaiting a drain event in .pipe()s
  this.awaitDrain = 0;

  // if true, a maybeReadMore has been scheduled
  this.readingMore = false;

  this.decoder = null;
  this.encoding = null;
  if (options.encoding) {
    if (!StringDecoder) StringDecoder = require('string_decoder/').StringDecoder;
    this.decoder = new StringDecoder(options.encoding);
    this.encoding = options.encoding;
  }
}

var Duplex;
function Readable(options) {
  Duplex = Duplex || require('./_stream_duplex');

  if (!(this instanceof Readable)) return new Readable(options);

  this._readableState = new ReadableState(options, this);

  // legacy
  this.readable = true;

  if (options && typeof options.read === 'function') this._read = options.read;

  Stream.call(this);
}

// Manually shove something into the read() buffer.
// This returns true if the highWaterMark has not been hit yet,
// similar to how Writable.write() returns true if you should
// write() some more.
Readable.prototype.push = function (chunk, encoding) {
  var state = this._readableState;

  if (!state.objectMode && typeof chunk === 'string') {
    encoding = encoding || state.defaultEncoding;
    if (encoding !== state.encoding) {
      chunk = bufferShim.from(chunk, encoding);
      encoding = '';
    }
  }

  return readableAddChunk(this, state, chunk, encoding, false);
};

// Unshift should *always* be something directly out of read()
Readable.prototype.unshift = function (chunk) {
  var state = this._readableState;
  return readableAddChunk(this, state, chunk, '', true);
};

Readable.prototype.isPaused = function () {
  return this._readableState.flowing === false;
};

function readableAddChunk(stream, state, chunk, encoding, addToFront) {
  var er = chunkInvalid(state, chunk);
  if (er) {
    stream.emit('error', er);
  } else if (chunk === null) {
    state.reading = false;
    onEofChunk(stream, state);
  } else if (state.objectMode || chunk && chunk.length > 0) {
    if (state.ended && !addToFront) {
      var e = new Error('stream.push() after EOF');
      stream.emit('error', e);
    } else if (state.endEmitted && addToFront) {
      var _e = new Error('stream.unshift() after end event');
      stream.emit('error', _e);
    } else {
      var skipAdd;
      if (state.decoder && !addToFront && !encoding) {
        chunk = state.decoder.write(chunk);
        skipAdd = !state.objectMode && chunk.length === 0;
      }

      if (!addToFront) state.reading = false;

      // Don't add to the buffer if we've decoded to an empty string chunk and
      // we're not in object mode
      if (!skipAdd) {
        // if we want the data now, just emit it.
        if (state.flowing && state.length === 0 && !state.sync) {
          stream.emit('data', chunk);
          stream.read(0);
        } else {
          // update the buffer info.
          state.length += state.objectMode ? 1 : chunk.length;
          if (addToFront) state.buffer.unshift(chunk);else state.buffer.push(chunk);

          if (state.needReadable) emitReadable(stream);
        }
      }

      maybeReadMore(stream, state);
    }
  } else if (!addToFront) {
    state.reading = false;
  }

  return needMoreData(state);
}

// if it's past the high water mark, we can push in some more.
// Also, if we have no data yet, we can stand some
// more bytes.  This is to work around cases where hwm=0,
// such as the repl.  Also, if the push() triggered a
// readable event, and the user called read(largeNumber) such that
// needReadable was set, then we ought to push more, so that another
// 'readable' event will be triggered.
function needMoreData(state) {
  return !state.ended && (state.needReadable || state.length < state.highWaterMark || state.length === 0);
}

// backwards compatibility.
Readable.prototype.setEncoding = function (enc) {
  if (!StringDecoder) StringDecoder = require('string_decoder/').StringDecoder;
  this._readableState.decoder = new StringDecoder(enc);
  this._readableState.encoding = enc;
  return this;
};

// Don't raise the hwm > 8MB
var MAX_HWM = 0x800000;
function computeNewHighWaterMark(n) {
  if (n >= MAX_HWM) {
    n = MAX_HWM;
  } else {
    // Get the next highest power of 2
    n--;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    n++;
  }
  return n;
}

function howMuchToRead(n, state) {
  if (state.length === 0 && state.ended) return 0;

  if (state.objectMode) return n === 0 ? 0 : 1;

  if (n === null || isNaN(n)) {
    // only flow one buffer at a time
    if (state.flowing && state.buffer.length) return state.buffer[0].length;else return state.length;
  }

  if (n <= 0) return 0;

  // If we're asking for more than the target buffer level,
  // then raise the water mark.  Bump up to the next highest
  // power of 2, to prevent increasing it excessively in tiny
  // amounts.
  if (n > state.highWaterMark) state.highWaterMark = computeNewHighWaterMark(n);

  // don't have that much.  return null, unless we've ended.
  if (n > state.length) {
    if (!state.ended) {
      state.needReadable = true;
      return 0;
    } else {
      return state.length;
    }
  }

  return n;
}

// you can override either this method, or the async _read(n) below.
Readable.prototype.read = function (n) {
  debug('read', n);
  var state = this._readableState;
  var nOrig = n;

  if (typeof n !== 'number' || n > 0) state.emittedReadable = false;

  // if we're doing read(0) to trigger a readable event, but we
  // already have a bunch of data in the buffer, then just trigger
  // the 'readable' event and move on.
  if (n === 0 && state.needReadable && (state.length >= state.highWaterMark || state.ended)) {
    debug('read: emitReadable', state.length, state.ended);
    if (state.length === 0 && state.ended) endReadable(this);else emitReadable(this);
    return null;
  }

  n = howMuchToRead(n, state);

  // if we've ended, and we're now clear, then finish it up.
  if (n === 0 && state.ended) {
    if (state.length === 0) endReadable(this);
    return null;
  }

  // All the actual chunk generation logic needs to be
  // *below* the call to _read.  The reason is that in certain
  // synthetic stream cases, such as passthrough streams, _read
  // may be a completely synchronous operation which may change
  // the state of the read buffer, providing enough data when
  // before there was *not* enough.
  //
  // So, the steps are:
  // 1. Figure out what the state of things will be after we do
  // a read from the buffer.
  //
  // 2. If that resulting state will trigger a _read, then call _read.
  // Note that this may be asynchronous, or synchronous.  Yes, it is
  // deeply ugly to write APIs this way, but that still doesn't mean
  // that the Readable class should behave improperly, as streams are
  // designed to be sync/async agnostic.
  // Take note if the _read call is sync or async (ie, if the read call
  // has returned yet), so that we know whether or not it's safe to emit
  // 'readable' etc.
  //
  // 3. Actually pull the requested chunks out of the buffer and return.

  // if we need a readable event, then we need to do some reading.
  var doRead = state.needReadable;
  debug('need readable', doRead);

  // if we currently have less than the highWaterMark, then also read some
  if (state.length === 0 || state.length - n < state.highWaterMark) {
    doRead = true;
    debug('length less than watermark', doRead);
  }

  // however, if we've ended, then there's no point, and if we're already
  // reading, then it's unnecessary.
  if (state.ended || state.reading) {
    doRead = false;
    debug('reading or ended', doRead);
  }

  if (doRead) {
    debug('do read');
    state.reading = true;
    state.sync = true;
    // if the length is currently zero, then we *need* a readable event.
    if (state.length === 0) state.needReadable = true;
    // call internal read method
    this._read(state.highWaterMark);
    state.sync = false;
  }

  // If _read pushed data synchronously, then `reading` will be false,
  // and we need to re-evaluate how much data we can return to the user.
  if (doRead && !state.reading) n = howMuchToRead(nOrig, state);

  var ret;
  if (n > 0) ret = fromList(n, state);else ret = null;

  if (ret === null) {
    state.needReadable = true;
    n = 0;
  }

  state.length -= n;

  // If we have nothing in the buffer, then we want to know
  // as soon as we *do* get something into the buffer.
  if (state.length === 0 && !state.ended) state.needReadable = true;

  // If we tried to read() past the EOF, then emit end on the next tick.
  if (nOrig !== n && state.ended && state.length === 0) endReadable(this);

  if (ret !== null) this.emit('data', ret);

  return ret;
};

function chunkInvalid(state, chunk) {
  var er = null;
  if (!Buffer.isBuffer(chunk) && typeof chunk !== 'string' && chunk !== null && chunk !== undefined && !state.objectMode) {
    er = new TypeError('Invalid non-string/buffer chunk');
  }
  return er;
}

function onEofChunk(stream, state) {
  if (state.ended) return;
  if (state.decoder) {
    var chunk = state.decoder.end();
    if (chunk && chunk.length) {
      state.buffer.push(chunk);
      state.length += state.objectMode ? 1 : chunk.length;
    }
  }
  state.ended = true;

  // emit 'readable' now to make sure it gets picked up.
  emitReadable(stream);
}

// Don't emit readable right away in sync mode, because this can trigger
// another read() call => stack overflow.  This way, it might trigger
// a nextTick recursion warning, but that's not so bad.
function emitReadable(stream) {
  var state = stream._readableState;
  state.needReadable = false;
  if (!state.emittedReadable) {
    debug('emitReadable', state.flowing);
    state.emittedReadable = true;
    if (state.sync) processNextTick(emitReadable_, stream);else emitReadable_(stream);
  }
}

function emitReadable_(stream) {
  debug('emit readable');
  stream.emit('readable');
  flow(stream);
}

// at this point, the user has presumably seen the 'readable' event,
// and called read() to consume some data.  that may have triggered
// in turn another _read(n) call, in which case reading = true if
// it's in progress.
// However, if we're not ended, or reading, and the length < hwm,
// then go ahead and try to read some more preemptively.
function maybeReadMore(stream, state) {
  if (!state.readingMore) {
    state.readingMore = true;
    processNextTick(maybeReadMore_, stream, state);
  }
}

function maybeReadMore_(stream, state) {
  var len = state.length;
  while (!state.reading && !state.flowing && !state.ended && state.length < state.highWaterMark) {
    debug('maybeReadMore read 0');
    stream.read(0);
    if (len === state.length)
      // didn't get any data, stop spinning.
      break;else len = state.length;
  }
  state.readingMore = false;
}

// abstract method.  to be overridden in specific implementation classes.
// call cb(er, data) where data is <= n in length.
// for virtual (non-string, non-buffer) streams, "length" is somewhat
// arbitrary, and perhaps not very meaningful.
Readable.prototype._read = function (n) {
  this.emit('error', new Error('not implemented'));
};

Readable.prototype.pipe = function (dest, pipeOpts) {
  var src = this;
  var state = this._readableState;

  switch (state.pipesCount) {
    case 0:
      state.pipes = dest;
      break;
    case 1:
      state.pipes = [state.pipes, dest];
      break;
    default:
      state.pipes.push(dest);
      break;
  }
  state.pipesCount += 1;
  debug('pipe count=%d opts=%j', state.pipesCount, pipeOpts);

  var doEnd = (!pipeOpts || pipeOpts.end !== false) && dest !== process.stdout && dest !== process.stderr;

  var endFn = doEnd ? onend : cleanup;
  if (state.endEmitted) processNextTick(endFn);else src.once('end', endFn);

  dest.on('unpipe', onunpipe);
  function onunpipe(readable) {
    debug('onunpipe');
    if (readable === src) {
      cleanup();
    }
  }

  function onend() {
    debug('onend');
    dest.end();
  }

  // when the dest drains, it reduces the awaitDrain counter
  // on the source.  This would be more elegant with a .once()
  // handler in flow(), but adding and removing repeatedly is
  // too slow.
  var ondrain = pipeOnDrain(src);
  dest.on('drain', ondrain);

  var cleanedUp = false;
  function cleanup() {
    debug('cleanup');
    // cleanup event handlers once the pipe is broken
    dest.removeListener('close', onclose);
    dest.removeListener('finish', onfinish);
    dest.removeListener('drain', ondrain);
    dest.removeListener('error', onerror);
    dest.removeListener('unpipe', onunpipe);
    src.removeListener('end', onend);
    src.removeListener('end', cleanup);
    src.removeListener('data', ondata);

    cleanedUp = true;

    // if the reader is waiting for a drain event from this
    // specific writer, then it would cause it to never start
    // flowing again.
    // So, if this is awaiting a drain, then we just call it now.
    // If we don't know, then assume that we are waiting for one.
    if (state.awaitDrain && (!dest._writableState || dest._writableState.needDrain)) ondrain();
  }

  src.on('data', ondata);
  function ondata(chunk) {
    debug('ondata');
    var ret = dest.write(chunk);
    if (false === ret) {
      // If the user unpiped during `dest.write()`, it is possible
      // to get stuck in a permanently paused state if that write
      // also returned false.
      // => Check whether `dest` is still a piping destination.
      if ((state.pipesCount === 1 && state.pipes === dest || state.pipesCount > 1 && indexOf(state.pipes, dest) !== -1) && !cleanedUp) {
        debug('false write response, pause', src._readableState.awaitDrain);
        src._readableState.awaitDrain++;
      }
      src.pause();
    }
  }

  // if the dest has an error, then stop piping into it.
  // however, don't suppress the throwing behavior for this.
  function onerror(er) {
    debug('onerror', er);
    unpipe();
    dest.removeListener('error', onerror);
    if (EElistenerCount(dest, 'error') === 0) dest.emit('error', er);
  }

  // Make sure our error handler is attached before userland ones.
  prependListener(dest, 'error', onerror);

  // Both close and finish should trigger unpipe, but only once.
  function onclose() {
    dest.removeListener('finish', onfinish);
    unpipe();
  }
  dest.once('close', onclose);
  function onfinish() {
    debug('onfinish');
    dest.removeListener('close', onclose);
    unpipe();
  }
  dest.once('finish', onfinish);

  function unpipe() {
    debug('unpipe');
    src.unpipe(dest);
  }

  // tell the dest that it's being piped to
  dest.emit('pipe', src);

  // start the flow if it hasn't been started already.
  if (!state.flowing) {
    debug('pipe resume');
    src.resume();
  }

  return dest;
};

function pipeOnDrain(src) {
  return function () {
    var state = src._readableState;
    debug('pipeOnDrain', state.awaitDrain);
    if (state.awaitDrain) state.awaitDrain--;
    if (state.awaitDrain === 0 && EElistenerCount(src, 'data')) {
      state.flowing = true;
      flow(src);
    }
  };
}

Readable.prototype.unpipe = function (dest) {
  var state = this._readableState;

  // if we're not piping anywhere, then do nothing.
  if (state.pipesCount === 0) return this;

  // just one destination.  most common case.
  if (state.pipesCount === 1) {
    // passed in one, but it's not the right one.
    if (dest && dest !== state.pipes) return this;

    if (!dest) dest = state.pipes;

    // got a match.
    state.pipes = null;
    state.pipesCount = 0;
    state.flowing = false;
    if (dest) dest.emit('unpipe', this);
    return this;
  }

  // slow case. multiple pipe destinations.

  if (!dest) {
    // remove all.
    var dests = state.pipes;
    var len = state.pipesCount;
    state.pipes = null;
    state.pipesCount = 0;
    state.flowing = false;

    for (var _i = 0; _i < len; _i++) {
      dests[_i].emit('unpipe', this);
    }return this;
  }

  // try to find the right one.
  var i = indexOf(state.pipes, dest);
  if (i === -1) return this;

  state.pipes.splice(i, 1);
  state.pipesCount -= 1;
  if (state.pipesCount === 1) state.pipes = state.pipes[0];

  dest.emit('unpipe', this);

  return this;
};

// set up data events if they are asked for
// Ensure readable listeners eventually get something
Readable.prototype.on = function (ev, fn) {
  var res = Stream.prototype.on.call(this, ev, fn);

  // If listening to data, and it has not explicitly been paused,
  // then call resume to start the flow of data on the next tick.
  if (ev === 'data' && false !== this._readableState.flowing) {
    this.resume();
  }

  if (ev === 'readable' && !this._readableState.endEmitted) {
    var state = this._readableState;
    if (!state.readableListening) {
      state.readableListening = true;
      state.emittedReadable = false;
      state.needReadable = true;
      if (!state.reading) {
        processNextTick(nReadingNextTick, this);
      } else if (state.length) {
        emitReadable(this, state);
      }
    }
  }

  return res;
};
Readable.prototype.addListener = Readable.prototype.on;

function nReadingNextTick(self) {
  debug('readable nexttick read 0');
  self.read(0);
}

// pause() and resume() are remnants of the legacy readable stream API
// If the user uses them, then switch into old mode.
Readable.prototype.resume = function () {
  var state = this._readableState;
  if (!state.flowing) {
    debug('resume');
    state.flowing = true;
    resume(this, state);
  }
  return this;
};

function resume(stream, state) {
  if (!state.resumeScheduled) {
    state.resumeScheduled = true;
    processNextTick(resume_, stream, state);
  }
}

function resume_(stream, state) {
  if (!state.reading) {
    debug('resume read 0');
    stream.read(0);
  }

  state.resumeScheduled = false;
  stream.emit('resume');
  flow(stream);
  if (state.flowing && !state.reading) stream.read(0);
}

Readable.prototype.pause = function () {
  debug('call pause flowing=%j', this._readableState.flowing);
  if (false !== this._readableState.flowing) {
    debug('pause');
    this._readableState.flowing = false;
    this.emit('pause');
  }
  return this;
};

function flow(stream) {
  var state = stream._readableState;
  debug('flow', state.flowing);
  if (state.flowing) {
    do {
      var chunk = stream.read();
    } while (null !== chunk && state.flowing);
  }
}

// wrap an old-style stream as the async data source.
// This is *not* part of the readable stream interface.
// It is an ugly unfortunate mess of history.
Readable.prototype.wrap = function (stream) {
  var state = this._readableState;
  var paused = false;

  var self = this;
  stream.on('end', function () {
    debug('wrapped end');
    if (state.decoder && !state.ended) {
      var chunk = state.decoder.end();
      if (chunk && chunk.length) self.push(chunk);
    }

    self.push(null);
  });

  stream.on('data', function (chunk) {
    debug('wrapped data');
    if (state.decoder) chunk = state.decoder.write(chunk);

    // don't skip over falsy values in objectMode
    if (state.objectMode && (chunk === null || chunk === undefined)) return;else if (!state.objectMode && (!chunk || !chunk.length)) return;

    var ret = self.push(chunk);
    if (!ret) {
      paused = true;
      stream.pause();
    }
  });

  // proxy all the other methods.
  // important when wrapping filters and duplexes.
  for (var i in stream) {
    if (this[i] === undefined && typeof stream[i] === 'function') {
      this[i] = function (method) {
        return function () {
          return stream[method].apply(stream, arguments);
        };
      }(i);
    }
  }

  // proxy certain important events.
  var events = ['error', 'close', 'destroy', 'pause', 'resume'];
  forEach(events, function (ev) {
    stream.on(ev, self.emit.bind(self, ev));
  });

  // when we try to consume some more bytes, simply unpause the
  // underlying stream.
  self._read = function (n) {
    debug('wrapped _read', n);
    if (paused) {
      paused = false;
      stream.resume();
    }
  };

  return self;
};

// exposed for testing purposes only.
Readable._fromList = fromList;

// Pluck off n bytes from an array of buffers.
// Length is the combined lengths of all the buffers in the list.
function fromList(n, state) {
  var list = state.buffer;
  var length = state.length;
  var stringMode = !!state.decoder;
  var objectMode = !!state.objectMode;
  var ret;

  // nothing in the list, definitely empty.
  if (list.length === 0) return null;

  if (length === 0) ret = null;else if (objectMode) ret = list.shift();else if (!n || n >= length) {
    // read it all, truncate the array.
    if (stringMode) ret = list.join('');else if (list.length === 1) ret = list[0];else ret = Buffer.concat(list, length);
    list.length = 0;
  } else {
    // read just some of it.
    if (n < list[0].length) {
      // just take a part of the first list item.
      // slice is the same for buffers and strings.
      var buf = list[0];
      ret = buf.slice(0, n);
      list[0] = buf.slice(n);
    } else if (n === list[0].length) {
      // first list is a perfect match
      ret = list.shift();
    } else {
      // complex case.
      // we have enough to cover it, but it spans past the first buffer.
      if (stringMode) ret = '';else ret = bufferShim.allocUnsafe(n);

      var c = 0;
      for (var i = 0, l = list.length; i < l && c < n; i++) {
        var _buf = list[0];
        var cpy = Math.min(n - c, _buf.length);

        if (stringMode) ret += _buf.slice(0, cpy);else _buf.copy(ret, c, 0, cpy);

        if (cpy < _buf.length) list[0] = _buf.slice(cpy);else list.shift();

        c += cpy;
      }
    }
  }

  return ret;
}

function endReadable(stream) {
  var state = stream._readableState;

  // If we get here before consuming all the bytes, then that is a
  // bug in node.  Should never happen.
  if (state.length > 0) throw new Error('"endReadable()" called on non-empty stream');

  if (!state.endEmitted) {
    state.ended = true;
    processNextTick(endReadableNT, state, stream);
  }
}

function endReadableNT(state, stream) {
  // Check that we didn't get one last unshift.
  if (!state.endEmitted && state.length === 0) {
    state.endEmitted = true;
    stream.readable = false;
    stream.emit('end');
  }
}

function forEach(xs, f) {
  for (var i = 0, l = xs.length; i < l; i++) {
    f(xs[i], i);
  }
}

function indexOf(xs, x) {
  for (var i = 0, l = xs.length; i < l; i++) {
    if (xs[i] === x) return i;
  }
  return -1;
}
}).call(this,require('_process'))
},{"./_stream_duplex":15,"_process":13,"buffer":4,"buffer-shims":3,"core-util-is":5,"events":6,"inherits":8,"isarray":10,"process-nextick-args":12,"string_decoder/":25,"util":2}],18:[function(require,module,exports){
// a transform stream is a readable/writable stream where you do
// something with the data.  Sometimes it's called a "filter",
// but that's not a great name for it, since that implies a thing where
// some bits pass through, and others are simply ignored.  (That would
// be a valid example of a transform, of course.)
//
// While the output is causally related to the input, it's not a
// necessarily symmetric or synchronous transformation.  For example,
// a zlib stream might take multiple plain-text writes(), and then
// emit a single compressed chunk some time in the future.
//
// Here's how this works:
//
// The Transform stream has all the aspects of the readable and writable
// stream classes.  When you write(chunk), that calls _write(chunk,cb)
// internally, and returns false if there's a lot of pending writes
// buffered up.  When you call read(), that calls _read(n) until
// there's enough pending readable data buffered up.
//
// In a transform stream, the written data is placed in a buffer.  When
// _read(n) is called, it transforms the queued up data, calling the
// buffered _write cb's as it consumes chunks.  If consuming a single
// written chunk would result in multiple output chunks, then the first
// outputted bit calls the readcb, and subsequent chunks just go into
// the read buffer, and will cause it to emit 'readable' if necessary.
//
// This way, back-pressure is actually determined by the reading side,
// since _read has to be called to start processing a new chunk.  However,
// a pathological inflate type of transform can cause excessive buffering
// here.  For example, imagine a stream where every byte of input is
// interpreted as an integer from 0-255, and then results in that many
// bytes of output.  Writing the 4 bytes {ff,ff,ff,ff} would result in
// 1kb of data being output.  In this case, you could write a very small
// amount of input, and end up with a very large amount of output.  In
// such a pathological inflating mechanism, there'd be no way to tell
// the system to stop doing the transform.  A single 4MB write could
// cause the system to run out of memory.
//
// However, even in such a pathological case, only a single written chunk
// would be consumed, and then the rest would wait (un-transformed) until
// the results of the previous transformed chunk were consumed.

'use strict';

module.exports = Transform;

var Duplex = require('./_stream_duplex');

/*<replacement>*/
var util = require('core-util-is');
util.inherits = require('inherits');
/*</replacement>*/

util.inherits(Transform, Duplex);

function TransformState(stream) {
  this.afterTransform = function (er, data) {
    return afterTransform(stream, er, data);
  };

  this.needTransform = false;
  this.transforming = false;
  this.writecb = null;
  this.writechunk = null;
  this.writeencoding = null;
}

function afterTransform(stream, er, data) {
  var ts = stream._transformState;
  ts.transforming = false;

  var cb = ts.writecb;

  if (!cb) return stream.emit('error', new Error('no writecb in Transform class'));

  ts.writechunk = null;
  ts.writecb = null;

  if (data !== null && data !== undefined) stream.push(data);

  cb(er);

  var rs = stream._readableState;
  rs.reading = false;
  if (rs.needReadable || rs.length < rs.highWaterMark) {
    stream._read(rs.highWaterMark);
  }
}

function Transform(options) {
  if (!(this instanceof Transform)) return new Transform(options);

  Duplex.call(this, options);

  this._transformState = new TransformState(this);

  // when the writable side finishes, then flush out anything remaining.
  var stream = this;

  // start out asking for a readable event once data is transformed.
  this._readableState.needReadable = true;

  // we have implemented the _read method, and done the other things
  // that Readable wants before the first _read call, so unset the
  // sync guard flag.
  this._readableState.sync = false;

  if (options) {
    if (typeof options.transform === 'function') this._transform = options.transform;

    if (typeof options.flush === 'function') this._flush = options.flush;
  }

  this.once('prefinish', function () {
    if (typeof this._flush === 'function') this._flush(function (er) {
      done(stream, er);
    });else done(stream);
  });
}

Transform.prototype.push = function (chunk, encoding) {
  this._transformState.needTransform = false;
  return Duplex.prototype.push.call(this, chunk, encoding);
};

// This is the part where you do stuff!
// override this function in implementation classes.
// 'chunk' is an input chunk.
//
// Call `push(newChunk)` to pass along transformed output
// to the readable side.  You may call 'push' zero or more times.
//
// Call `cb(err)` when you are done with this chunk.  If you pass
// an error, then that'll put the hurt on the whole operation.  If you
// never call cb(), then you'll never get another chunk.
Transform.prototype._transform = function (chunk, encoding, cb) {
  throw new Error('Not implemented');
};

Transform.prototype._write = function (chunk, encoding, cb) {
  var ts = this._transformState;
  ts.writecb = cb;
  ts.writechunk = chunk;
  ts.writeencoding = encoding;
  if (!ts.transforming) {
    var rs = this._readableState;
    if (ts.needTransform || rs.needReadable || rs.length < rs.highWaterMark) this._read(rs.highWaterMark);
  }
};

// Doesn't matter what the args are here.
// _transform does all the work.
// That we got here means that the readable side wants more data.
Transform.prototype._read = function (n) {
  var ts = this._transformState;

  if (ts.writechunk !== null && ts.writecb && !ts.transforming) {
    ts.transforming = true;
    this._transform(ts.writechunk, ts.writeencoding, ts.afterTransform);
  } else {
    // mark that we need a transform, so that any data that comes in
    // will get processed, now that we've asked for it.
    ts.needTransform = true;
  }
};

function done(stream, er) {
  if (er) return stream.emit('error', er);

  // if there's nothing in the write buffer, then that means
  // that nothing more will ever be provided
  var ws = stream._writableState;
  var ts = stream._transformState;

  if (ws.length) throw new Error('Calling transform done when ws.length != 0');

  if (ts.transforming) throw new Error('Calling transform done when still transforming');

  return stream.push(null);
}
},{"./_stream_duplex":15,"core-util-is":5,"inherits":8}],19:[function(require,module,exports){
(function (process){
// A bit simpler than readable streams.
// Implement an async ._write(chunk, encoding, cb), and it'll handle all
// the drain event emission and buffering.

'use strict';

module.exports = Writable;

/*<replacement>*/
var processNextTick = require('process-nextick-args');
/*</replacement>*/

/*<replacement>*/
var asyncWrite = !process.browser && ['v0.10', 'v0.9.'].indexOf(process.version.slice(0, 5)) > -1 ? setImmediate : processNextTick;
/*</replacement>*/

Writable.WritableState = WritableState;

/*<replacement>*/
var util = require('core-util-is');
util.inherits = require('inherits');
/*</replacement>*/

/*<replacement>*/
var internalUtil = {
  deprecate: require('util-deprecate')
};
/*</replacement>*/

/*<replacement>*/
var Stream;
(function () {
  try {
    Stream = require('st' + 'ream');
  } catch (_) {} finally {
    if (!Stream) Stream = require('events').EventEmitter;
  }
})();
/*</replacement>*/

var Buffer = require('buffer').Buffer;
/*<replacement>*/
var bufferShim = require('buffer-shims');
/*</replacement>*/

util.inherits(Writable, Stream);

function nop() {}

function WriteReq(chunk, encoding, cb) {
  this.chunk = chunk;
  this.encoding = encoding;
  this.callback = cb;
  this.next = null;
}

var Duplex;
function WritableState(options, stream) {
  Duplex = Duplex || require('./_stream_duplex');

  options = options || {};

  // object stream flag to indicate whether or not this stream
  // contains buffers or objects.
  this.objectMode = !!options.objectMode;

  if (stream instanceof Duplex) this.objectMode = this.objectMode || !!options.writableObjectMode;

  // the point at which write() starts returning false
  // Note: 0 is a valid value, means that we always return false if
  // the entire buffer is not flushed immediately on write()
  var hwm = options.highWaterMark;
  var defaultHwm = this.objectMode ? 16 : 16 * 1024;
  this.highWaterMark = hwm || hwm === 0 ? hwm : defaultHwm;

  // cast to ints.
  this.highWaterMark = ~ ~this.highWaterMark;

  this.needDrain = false;
  // at the start of calling end()
  this.ending = false;
  // when end() has been called, and returned
  this.ended = false;
  // when 'finish' is emitted
  this.finished = false;

  // should we decode strings into buffers before passing to _write?
  // this is here so that some node-core streams can optimize string
  // handling at a lower level.
  var noDecode = options.decodeStrings === false;
  this.decodeStrings = !noDecode;

  // Crypto is kind of old and crusty.  Historically, its default string
  // encoding is 'binary' so we have to make this configurable.
  // Everything else in the universe uses 'utf8', though.
  this.defaultEncoding = options.defaultEncoding || 'utf8';

  // not an actual buffer we keep track of, but a measurement
  // of how much we're waiting to get pushed to some underlying
  // socket or file.
  this.length = 0;

  // a flag to see when we're in the middle of a write.
  this.writing = false;

  // when true all writes will be buffered until .uncork() call
  this.corked = 0;

  // a flag to be able to tell if the onwrite cb is called immediately,
  // or on a later tick.  We set this to true at first, because any
  // actions that shouldn't happen until "later" should generally also
  // not happen before the first write call.
  this.sync = true;

  // a flag to know if we're processing previously buffered items, which
  // may call the _write() callback in the same tick, so that we don't
  // end up in an overlapped onwrite situation.
  this.bufferProcessing = false;

  // the callback that's passed to _write(chunk,cb)
  this.onwrite = function (er) {
    onwrite(stream, er);
  };

  // the callback that the user supplies to write(chunk,encoding,cb)
  this.writecb = null;

  // the amount that is being written when _write is called.
  this.writelen = 0;

  this.bufferedRequest = null;
  this.lastBufferedRequest = null;

  // number of pending user-supplied write callbacks
  // this must be 0 before 'finish' can be emitted
  this.pendingcb = 0;

  // emit prefinish if the only thing we're waiting for is _write cbs
  // This is relevant for synchronous Transform streams
  this.prefinished = false;

  // True if the error was already emitted and should not be thrown again
  this.errorEmitted = false;

  // count buffered requests
  this.bufferedRequestCount = 0;

  // allocate the first CorkedRequest, there is always
  // one allocated and free to use, and we maintain at most two
  this.corkedRequestsFree = new CorkedRequest(this);
}

WritableState.prototype.getBuffer = function writableStateGetBuffer() {
  var current = this.bufferedRequest;
  var out = [];
  while (current) {
    out.push(current);
    current = current.next;
  }
  return out;
};

(function () {
  try {
    Object.defineProperty(WritableState.prototype, 'buffer', {
      get: internalUtil.deprecate(function () {
        return this.getBuffer();
      }, '_writableState.buffer is deprecated. Use _writableState.getBuffer ' + 'instead.')
    });
  } catch (_) {}
})();

var Duplex;
function Writable(options) {
  Duplex = Duplex || require('./_stream_duplex');

  // Writable ctor is applied to Duplexes, though they're not
  // instanceof Writable, they're instanceof Readable.
  if (!(this instanceof Writable) && !(this instanceof Duplex)) return new Writable(options);

  this._writableState = new WritableState(options, this);

  // legacy.
  this.writable = true;

  if (options) {
    if (typeof options.write === 'function') this._write = options.write;

    if (typeof options.writev === 'function') this._writev = options.writev;
  }

  Stream.call(this);
}

// Otherwise people can pipe Writable streams, which is just wrong.
Writable.prototype.pipe = function () {
  this.emit('error', new Error('Cannot pipe, not readable'));
};

function writeAfterEnd(stream, cb) {
  var er = new Error('write after end');
  // TODO: defer error events consistently everywhere, not just the cb
  stream.emit('error', er);
  processNextTick(cb, er);
}

// If we get something that is not a buffer, string, null, or undefined,
// and we're not in objectMode, then that's an error.
// Otherwise stream chunks are all considered to be of length=1, and the
// watermarks determine how many objects to keep in the buffer, rather than
// how many bytes or characters.
function validChunk(stream, state, chunk, cb) {
  var valid = true;
  var er = false;
  // Always throw error if a null is written
  // if we are not in object mode then throw
  // if it is not a buffer, string, or undefined.
  if (chunk === null) {
    er = new TypeError('May not write null values to stream');
  } else if (!Buffer.isBuffer(chunk) && typeof chunk !== 'string' && chunk !== undefined && !state.objectMode) {
    er = new TypeError('Invalid non-string/buffer chunk');
  }
  if (er) {
    stream.emit('error', er);
    processNextTick(cb, er);
    valid = false;
  }
  return valid;
}

Writable.prototype.write = function (chunk, encoding, cb) {
  var state = this._writableState;
  var ret = false;

  if (typeof encoding === 'function') {
    cb = encoding;
    encoding = null;
  }

  if (Buffer.isBuffer(chunk)) encoding = 'buffer';else if (!encoding) encoding = state.defaultEncoding;

  if (typeof cb !== 'function') cb = nop;

  if (state.ended) writeAfterEnd(this, cb);else if (validChunk(this, state, chunk, cb)) {
    state.pendingcb++;
    ret = writeOrBuffer(this, state, chunk, encoding, cb);
  }

  return ret;
};

Writable.prototype.cork = function () {
  var state = this._writableState;

  state.corked++;
};

Writable.prototype.uncork = function () {
  var state = this._writableState;

  if (state.corked) {
    state.corked--;

    if (!state.writing && !state.corked && !state.finished && !state.bufferProcessing && state.bufferedRequest) clearBuffer(this, state);
  }
};

Writable.prototype.setDefaultEncoding = function setDefaultEncoding(encoding) {
  // node::ParseEncoding() requires lower case.
  if (typeof encoding === 'string') encoding = encoding.toLowerCase();
  if (!(['hex', 'utf8', 'utf-8', 'ascii', 'binary', 'base64', 'ucs2', 'ucs-2', 'utf16le', 'utf-16le', 'raw'].indexOf((encoding + '').toLowerCase()) > -1)) throw new TypeError('Unknown encoding: ' + encoding);
  this._writableState.defaultEncoding = encoding;
  return this;
};

function decodeChunk(state, chunk, encoding) {
  if (!state.objectMode && state.decodeStrings !== false && typeof chunk === 'string') {
    chunk = bufferShim.from(chunk, encoding);
  }
  return chunk;
}

// if we're already writing something, then just put this
// in the queue, and wait our turn.  Otherwise, call _write
// If we return false, then we need a drain event, so set that flag.
function writeOrBuffer(stream, state, chunk, encoding, cb) {
  chunk = decodeChunk(state, chunk, encoding);

  if (Buffer.isBuffer(chunk)) encoding = 'buffer';
  var len = state.objectMode ? 1 : chunk.length;

  state.length += len;

  var ret = state.length < state.highWaterMark;
  // we must ensure that previous needDrain will not be reset to false.
  if (!ret) state.needDrain = true;

  if (state.writing || state.corked) {
    var last = state.lastBufferedRequest;
    state.lastBufferedRequest = new WriteReq(chunk, encoding, cb);
    if (last) {
      last.next = state.lastBufferedRequest;
    } else {
      state.bufferedRequest = state.lastBufferedRequest;
    }
    state.bufferedRequestCount += 1;
  } else {
    doWrite(stream, state, false, len, chunk, encoding, cb);
  }

  return ret;
}

function doWrite(stream, state, writev, len, chunk, encoding, cb) {
  state.writelen = len;
  state.writecb = cb;
  state.writing = true;
  state.sync = true;
  if (writev) stream._writev(chunk, state.onwrite);else stream._write(chunk, encoding, state.onwrite);
  state.sync = false;
}

function onwriteError(stream, state, sync, er, cb) {
  --state.pendingcb;
  if (sync) processNextTick(cb, er);else cb(er);

  stream._writableState.errorEmitted = true;
  stream.emit('error', er);
}

function onwriteStateUpdate(state) {
  state.writing = false;
  state.writecb = null;
  state.length -= state.writelen;
  state.writelen = 0;
}

function onwrite(stream, er) {
  var state = stream._writableState;
  var sync = state.sync;
  var cb = state.writecb;

  onwriteStateUpdate(state);

  if (er) onwriteError(stream, state, sync, er, cb);else {
    // Check if we're actually ready to finish, but don't emit yet
    var finished = needFinish(state);

    if (!finished && !state.corked && !state.bufferProcessing && state.bufferedRequest) {
      clearBuffer(stream, state);
    }

    if (sync) {
      /*<replacement>*/
      asyncWrite(afterWrite, stream, state, finished, cb);
      /*</replacement>*/
    } else {
        afterWrite(stream, state, finished, cb);
      }
  }
}

function afterWrite(stream, state, finished, cb) {
  if (!finished) onwriteDrain(stream, state);
  state.pendingcb--;
  cb();
  finishMaybe(stream, state);
}

// Must force callback to be called on nextTick, so that we don't
// emit 'drain' before the write() consumer gets the 'false' return
// value, and has a chance to attach a 'drain' listener.
function onwriteDrain(stream, state) {
  if (state.length === 0 && state.needDrain) {
    state.needDrain = false;
    stream.emit('drain');
  }
}

// if there's something in the buffer waiting, then process it
function clearBuffer(stream, state) {
  state.bufferProcessing = true;
  var entry = state.bufferedRequest;

  if (stream._writev && entry && entry.next) {
    // Fast case, write everything using _writev()
    var l = state.bufferedRequestCount;
    var buffer = new Array(l);
    var holder = state.corkedRequestsFree;
    holder.entry = entry;

    var count = 0;
    while (entry) {
      buffer[count] = entry;
      entry = entry.next;
      count += 1;
    }

    doWrite(stream, state, true, state.length, buffer, '', holder.finish);

    // doWrite is almost always async, defer these to save a bit of time
    // as the hot path ends with doWrite
    state.pendingcb++;
    state.lastBufferedRequest = null;
    if (holder.next) {
      state.corkedRequestsFree = holder.next;
      holder.next = null;
    } else {
      state.corkedRequestsFree = new CorkedRequest(state);
    }
  } else {
    // Slow case, write chunks one-by-one
    while (entry) {
      var chunk = entry.chunk;
      var encoding = entry.encoding;
      var cb = entry.callback;
      var len = state.objectMode ? 1 : chunk.length;

      doWrite(stream, state, false, len, chunk, encoding, cb);
      entry = entry.next;
      // if we didn't call the onwrite immediately, then
      // it means that we need to wait until it does.
      // also, that means that the chunk and cb are currently
      // being processed, so move the buffer counter past them.
      if (state.writing) {
        break;
      }
    }

    if (entry === null) state.lastBufferedRequest = null;
  }

  state.bufferedRequestCount = 0;
  state.bufferedRequest = entry;
  state.bufferProcessing = false;
}

Writable.prototype._write = function (chunk, encoding, cb) {
  cb(new Error('not implemented'));
};

Writable.prototype._writev = null;

Writable.prototype.end = function (chunk, encoding, cb) {
  var state = this._writableState;

  if (typeof chunk === 'function') {
    cb = chunk;
    chunk = null;
    encoding = null;
  } else if (typeof encoding === 'function') {
    cb = encoding;
    encoding = null;
  }

  if (chunk !== null && chunk !== undefined) this.write(chunk, encoding);

  // .end() fully uncorks
  if (state.corked) {
    state.corked = 1;
    this.uncork();
  }

  // ignore unnecessary end() calls.
  if (!state.ending && !state.finished) endWritable(this, state, cb);
};

function needFinish(state) {
  return state.ending && state.length === 0 && state.bufferedRequest === null && !state.finished && !state.writing;
}

function prefinish(stream, state) {
  if (!state.prefinished) {
    state.prefinished = true;
    stream.emit('prefinish');
  }
}

function finishMaybe(stream, state) {
  var need = needFinish(state);
  if (need) {
    if (state.pendingcb === 0) {
      prefinish(stream, state);
      state.finished = true;
      stream.emit('finish');
    } else {
      prefinish(stream, state);
    }
  }
  return need;
}

function endWritable(stream, state, cb) {
  state.ending = true;
  finishMaybe(stream, state);
  if (cb) {
    if (state.finished) processNextTick(cb);else stream.once('finish', cb);
  }
  state.ended = true;
  stream.writable = false;
}

// It seems a linked list but it is not
// there will be only 2 of these for each stream
function CorkedRequest(state) {
  var _this = this;

  this.next = null;
  this.entry = null;

  this.finish = function (err) {
    var entry = _this.entry;
    _this.entry = null;
    while (entry) {
      var cb = entry.callback;
      state.pendingcb--;
      cb(err);
      entry = entry.next;
    }
    if (state.corkedRequestsFree) {
      state.corkedRequestsFree.next = _this;
    } else {
      state.corkedRequestsFree = _this;
    }
  };
}
}).call(this,require('_process'))
},{"./_stream_duplex":15,"_process":13,"buffer":4,"buffer-shims":3,"core-util-is":5,"events":6,"inherits":8,"process-nextick-args":12,"util-deprecate":26}],20:[function(require,module,exports){
module.exports = require("./lib/_stream_passthrough.js")

},{"./lib/_stream_passthrough.js":16}],21:[function(require,module,exports){
(function (process){
var Stream = (function (){
  try {
    return require('st' + 'ream'); // hack to fix a circular dependency issue when used with browserify
  } catch(_){}
}());
exports = module.exports = require('./lib/_stream_readable.js');
exports.Stream = Stream || exports;
exports.Readable = exports;
exports.Writable = require('./lib/_stream_writable.js');
exports.Duplex = require('./lib/_stream_duplex.js');
exports.Transform = require('./lib/_stream_transform.js');
exports.PassThrough = require('./lib/_stream_passthrough.js');

if (!process.browser && process.env.READABLE_STREAM === 'disable' && Stream) {
  module.exports = Stream;
}

}).call(this,require('_process'))
},{"./lib/_stream_duplex.js":15,"./lib/_stream_passthrough.js":16,"./lib/_stream_readable.js":17,"./lib/_stream_transform.js":18,"./lib/_stream_writable.js":19,"_process":13}],22:[function(require,module,exports){
module.exports = require("./lib/_stream_transform.js")

},{"./lib/_stream_transform.js":18}],23:[function(require,module,exports){
module.exports = require("./lib/_stream_writable.js")

},{"./lib/_stream_writable.js":19}],24:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

module.exports = Stream;

var EE = require('events').EventEmitter;
var inherits = require('inherits');

inherits(Stream, EE);
Stream.Readable = require('readable-stream/readable.js');
Stream.Writable = require('readable-stream/writable.js');
Stream.Duplex = require('readable-stream/duplex.js');
Stream.Transform = require('readable-stream/transform.js');
Stream.PassThrough = require('readable-stream/passthrough.js');

// Backwards-compat with node 0.4.x
Stream.Stream = Stream;



// old-style streams.  Note that the pipe method (the only relevant
// part of this class) is overridden in the Readable class.

function Stream() {
  EE.call(this);
}

Stream.prototype.pipe = function(dest, options) {
  var source = this;

  function ondata(chunk) {
    if (dest.writable) {
      if (false === dest.write(chunk) && source.pause) {
        source.pause();
      }
    }
  }

  source.on('data', ondata);

  function ondrain() {
    if (source.readable && source.resume) {
      source.resume();
    }
  }

  dest.on('drain', ondrain);

  // If the 'end' option is not supplied, dest.end() will be called when
  // source gets the 'end' or 'close' events.  Only dest.end() once.
  if (!dest._isStdio && (!options || options.end !== false)) {
    source.on('end', onend);
    source.on('close', onclose);
  }

  var didOnEnd = false;
  function onend() {
    if (didOnEnd) return;
    didOnEnd = true;

    dest.end();
  }


  function onclose() {
    if (didOnEnd) return;
    didOnEnd = true;

    if (typeof dest.destroy === 'function') dest.destroy();
  }

  // don't leave dangling pipes when there are errors.
  function onerror(er) {
    cleanup();
    if (EE.listenerCount(this, 'error') === 0) {
      throw er; // Unhandled stream error in pipe.
    }
  }

  source.on('error', onerror);
  dest.on('error', onerror);

  // remove all the event listeners that were added.
  function cleanup() {
    source.removeListener('data', ondata);
    dest.removeListener('drain', ondrain);

    source.removeListener('end', onend);
    source.removeListener('close', onclose);

    source.removeListener('error', onerror);
    dest.removeListener('error', onerror);

    source.removeListener('end', cleanup);
    source.removeListener('close', cleanup);

    dest.removeListener('close', cleanup);
  }

  source.on('end', cleanup);
  source.on('close', cleanup);

  dest.on('close', cleanup);

  dest.emit('pipe', source);

  // Allow for unix-like usage: A.pipe(B).pipe(C)
  return dest;
};

},{"events":6,"inherits":8,"readable-stream/duplex.js":14,"readable-stream/passthrough.js":20,"readable-stream/readable.js":21,"readable-stream/transform.js":22,"readable-stream/writable.js":23}],25:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var Buffer = require('buffer').Buffer;

var isBufferEncoding = Buffer.isEncoding
  || function(encoding) {
       switch (encoding && encoding.toLowerCase()) {
         case 'hex': case 'utf8': case 'utf-8': case 'ascii': case 'binary': case 'base64': case 'ucs2': case 'ucs-2': case 'utf16le': case 'utf-16le': case 'raw': return true;
         default: return false;
       }
     }


function assertEncoding(encoding) {
  if (encoding && !isBufferEncoding(encoding)) {
    throw new Error('Unknown encoding: ' + encoding);
  }
}

// StringDecoder provides an interface for efficiently splitting a series of
// buffers into a series of JS strings without breaking apart multi-byte
// characters. CESU-8 is handled as part of the UTF-8 encoding.
//
// @TODO Handling all encodings inside a single object makes it very difficult
// to reason about this code, so it should be split up in the future.
// @TODO There should be a utf8-strict encoding that rejects invalid UTF-8 code
// points as used by CESU-8.
var StringDecoder = exports.StringDecoder = function(encoding) {
  this.encoding = (encoding || 'utf8').toLowerCase().replace(/[-_]/, '');
  assertEncoding(encoding);
  switch (this.encoding) {
    case 'utf8':
      // CESU-8 represents each of Surrogate Pair by 3-bytes
      this.surrogateSize = 3;
      break;
    case 'ucs2':
    case 'utf16le':
      // UTF-16 represents each of Surrogate Pair by 2-bytes
      this.surrogateSize = 2;
      this.detectIncompleteChar = utf16DetectIncompleteChar;
      break;
    case 'base64':
      // Base-64 stores 3 bytes in 4 chars, and pads the remainder.
      this.surrogateSize = 3;
      this.detectIncompleteChar = base64DetectIncompleteChar;
      break;
    default:
      this.write = passThroughWrite;
      return;
  }

  // Enough space to store all bytes of a single character. UTF-8 needs 4
  // bytes, but CESU-8 may require up to 6 (3 bytes per surrogate).
  this.charBuffer = new Buffer(6);
  // Number of bytes received for the current incomplete multi-byte character.
  this.charReceived = 0;
  // Number of bytes expected for the current incomplete multi-byte character.
  this.charLength = 0;
};


// write decodes the given buffer and returns it as JS string that is
// guaranteed to not contain any partial multi-byte characters. Any partial
// character found at the end of the buffer is buffered up, and will be
// returned when calling write again with the remaining bytes.
//
// Note: Converting a Buffer containing an orphan surrogate to a String
// currently works, but converting a String to a Buffer (via `new Buffer`, or
// Buffer#write) will replace incomplete surrogates with the unicode
// replacement character. See https://codereview.chromium.org/121173009/ .
StringDecoder.prototype.write = function(buffer) {
  var charStr = '';
  // if our last write ended with an incomplete multibyte character
  while (this.charLength) {
    // determine how many remaining bytes this buffer has to offer for this char
    var available = (buffer.length >= this.charLength - this.charReceived) ?
        this.charLength - this.charReceived :
        buffer.length;

    // add the new bytes to the char buffer
    buffer.copy(this.charBuffer, this.charReceived, 0, available);
    this.charReceived += available;

    if (this.charReceived < this.charLength) {
      // still not enough chars in this buffer? wait for more ...
      return '';
    }

    // remove bytes belonging to the current character from the buffer
    buffer = buffer.slice(available, buffer.length);

    // get the character that was split
    charStr = this.charBuffer.slice(0, this.charLength).toString(this.encoding);

    // CESU-8: lead surrogate (D800-DBFF) is also the incomplete character
    var charCode = charStr.charCodeAt(charStr.length - 1);
    if (charCode >= 0xD800 && charCode <= 0xDBFF) {
      this.charLength += this.surrogateSize;
      charStr = '';
      continue;
    }
    this.charReceived = this.charLength = 0;

    // if there are no more bytes in this buffer, just emit our char
    if (buffer.length === 0) {
      return charStr;
    }
    break;
  }

  // determine and set charLength / charReceived
  this.detectIncompleteChar(buffer);

  var end = buffer.length;
  if (this.charLength) {
    // buffer the incomplete character bytes we got
    buffer.copy(this.charBuffer, 0, buffer.length - this.charReceived, end);
    end -= this.charReceived;
  }

  charStr += buffer.toString(this.encoding, 0, end);

  var end = charStr.length - 1;
  var charCode = charStr.charCodeAt(end);
  // CESU-8: lead surrogate (D800-DBFF) is also the incomplete character
  if (charCode >= 0xD800 && charCode <= 0xDBFF) {
    var size = this.surrogateSize;
    this.charLength += size;
    this.charReceived += size;
    this.charBuffer.copy(this.charBuffer, size, 0, size);
    buffer.copy(this.charBuffer, 0, 0, size);
    return charStr.substring(0, end);
  }

  // or just emit the charStr
  return charStr;
};

// detectIncompleteChar determines if there is an incomplete UTF-8 character at
// the end of the given buffer. If so, it sets this.charLength to the byte
// length that character, and sets this.charReceived to the number of bytes
// that are available for this character.
StringDecoder.prototype.detectIncompleteChar = function(buffer) {
  // determine how many bytes we have to check at the end of this buffer
  var i = (buffer.length >= 3) ? 3 : buffer.length;

  // Figure out if one of the last i bytes of our buffer announces an
  // incomplete char.
  for (; i > 0; i--) {
    var c = buffer[buffer.length - i];

    // See http://en.wikipedia.org/wiki/UTF-8#Description

    // 110XXXXX
    if (i == 1 && c >> 5 == 0x06) {
      this.charLength = 2;
      break;
    }

    // 1110XXXX
    if (i <= 2 && c >> 4 == 0x0E) {
      this.charLength = 3;
      break;
    }

    // 11110XXX
    if (i <= 3 && c >> 3 == 0x1E) {
      this.charLength = 4;
      break;
    }
  }
  this.charReceived = i;
};

StringDecoder.prototype.end = function(buffer) {
  var res = '';
  if (buffer && buffer.length)
    res = this.write(buffer);

  if (this.charReceived) {
    var cr = this.charReceived;
    var buf = this.charBuffer;
    var enc = this.encoding;
    res += buf.slice(0, cr).toString(enc);
  }

  return res;
};

function passThroughWrite(buffer) {
  return buffer.toString(this.encoding);
}

function utf16DetectIncompleteChar(buffer) {
  this.charReceived = buffer.length % 2;
  this.charLength = this.charReceived ? 2 : 0;
}

function base64DetectIncompleteChar(buffer) {
  this.charReceived = buffer.length % 3;
  this.charLength = this.charReceived ? 3 : 0;
}

},{"buffer":4}],26:[function(require,module,exports){
(function (global){

/**
 * Module exports.
 */

module.exports = deprecate;

/**
 * Mark that a method should not be used.
 * Returns a modified function which warns once by default.
 *
 * If `localStorage.noDeprecation = true` is set, then it is a no-op.
 *
 * If `localStorage.throwDeprecation = true` is set, then deprecated functions
 * will throw an Error when invoked.
 *
 * If `localStorage.traceDeprecation = true` is set, then deprecated functions
 * will invoke `console.trace()` instead of `console.error()`.
 *
 * @param {Function} fn - the function to deprecate
 * @param {String} msg - the string to print to the console when `fn` is invoked
 * @returns {Function} a new "deprecated" version of `fn`
 * @api public
 */

function deprecate (fn, msg) {
  if (config('noDeprecation')) {
    return fn;
  }

  var warned = false;
  function deprecated() {
    if (!warned) {
      if (config('throwDeprecation')) {
        throw new Error(msg);
      } else if (config('traceDeprecation')) {
        console.trace(msg);
      } else {
        console.warn(msg);
      }
      warned = true;
    }
    return fn.apply(this, arguments);
  }

  return deprecated;
}

/**
 * Checks `localStorage` for boolean values for the given `name`.
 *
 * @param {String} name
 * @returns {Boolean}
 * @api private
 */

function config (name) {
  // accessing global.localStorage can trigger a DOMException in sandboxed iframes
  try {
    if (!global.localStorage) return false;
  } catch (_) {
    return false;
  }
  var val = global.localStorage[name];
  if (null == val) return false;
  return String(val).toLowerCase() === 'true';
}

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{}],27:[function(require,module,exports){
module.exports = function isBuffer(arg) {
  return arg && typeof arg === 'object'
    && typeof arg.copy === 'function'
    && typeof arg.fill === 'function'
    && typeof arg.readUInt8 === 'function';
}
},{}],28:[function(require,module,exports){
(function (process,global){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var formatRegExp = /%[sdj%]/g;
exports.format = function(f) {
  if (!isString(f)) {
    var objects = [];
    for (var i = 0; i < arguments.length; i++) {
      objects.push(inspect(arguments[i]));
    }
    return objects.join(' ');
  }

  var i = 1;
  var args = arguments;
  var len = args.length;
  var str = String(f).replace(formatRegExp, function(x) {
    if (x === '%%') return '%';
    if (i >= len) return x;
    switch (x) {
      case '%s': return String(args[i++]);
      case '%d': return Number(args[i++]);
      case '%j':
        try {
          return JSON.stringify(args[i++]);
        } catch (_) {
          return '[Circular]';
        }
      default:
        return x;
    }
  });
  for (var x = args[i]; i < len; x = args[++i]) {
    if (isNull(x) || !isObject(x)) {
      str += ' ' + x;
    } else {
      str += ' ' + inspect(x);
    }
  }
  return str;
};


// Mark that a method should not be used.
// Returns a modified function which warns once by default.
// If --no-deprecation is set, then it is a no-op.
exports.deprecate = function(fn, msg) {
  // Allow for deprecating things in the process of starting up.
  if (isUndefined(global.process)) {
    return function() {
      return exports.deprecate(fn, msg).apply(this, arguments);
    };
  }

  if (process.noDeprecation === true) {
    return fn;
  }

  var warned = false;
  function deprecated() {
    if (!warned) {
      if (process.throwDeprecation) {
        throw new Error(msg);
      } else if (process.traceDeprecation) {
        console.trace(msg);
      } else {
        console.error(msg);
      }
      warned = true;
    }
    return fn.apply(this, arguments);
  }

  return deprecated;
};


var debugs = {};
var debugEnviron;
exports.debuglog = function(set) {
  if (isUndefined(debugEnviron))
    debugEnviron = process.env.NODE_DEBUG || '';
  set = set.toUpperCase();
  if (!debugs[set]) {
    if (new RegExp('\\b' + set + '\\b', 'i').test(debugEnviron)) {
      var pid = process.pid;
      debugs[set] = function() {
        var msg = exports.format.apply(exports, arguments);
        console.error('%s %d: %s', set, pid, msg);
      };
    } else {
      debugs[set] = function() {};
    }
  }
  return debugs[set];
};


/**
 * Echos the value of a value. Trys to print the value out
 * in the best way possible given the different types.
 *
 * @param {Object} obj The object to print out.
 * @param {Object} opts Optional options object that alters the output.
 */
/* legacy: obj, showHidden, depth, colors*/
function inspect(obj, opts) {
  // default options
  var ctx = {
    seen: [],
    stylize: stylizeNoColor
  };
  // legacy...
  if (arguments.length >= 3) ctx.depth = arguments[2];
  if (arguments.length >= 4) ctx.colors = arguments[3];
  if (isBoolean(opts)) {
    // legacy...
    ctx.showHidden = opts;
  } else if (opts) {
    // got an "options" object
    exports._extend(ctx, opts);
  }
  // set default options
  if (isUndefined(ctx.showHidden)) ctx.showHidden = false;
  if (isUndefined(ctx.depth)) ctx.depth = 2;
  if (isUndefined(ctx.colors)) ctx.colors = false;
  if (isUndefined(ctx.customInspect)) ctx.customInspect = true;
  if (ctx.colors) ctx.stylize = stylizeWithColor;
  return formatValue(ctx, obj, ctx.depth);
}
exports.inspect = inspect;


// http://en.wikipedia.org/wiki/ANSI_escape_code#graphics
inspect.colors = {
  'bold' : [1, 22],
  'italic' : [3, 23],
  'underline' : [4, 24],
  'inverse' : [7, 27],
  'white' : [37, 39],
  'grey' : [90, 39],
  'black' : [30, 39],
  'blue' : [34, 39],
  'cyan' : [36, 39],
  'green' : [32, 39],
  'magenta' : [35, 39],
  'red' : [31, 39],
  'yellow' : [33, 39]
};

// Don't use 'blue' not visible on cmd.exe
inspect.styles = {
  'special': 'cyan',
  'number': 'yellow',
  'boolean': 'yellow',
  'undefined': 'grey',
  'null': 'bold',
  'string': 'green',
  'date': 'magenta',
  // "name": intentionally not styling
  'regexp': 'red'
};


function stylizeWithColor(str, styleType) {
  var style = inspect.styles[styleType];

  if (style) {
    return '\u001b[' + inspect.colors[style][0] + 'm' + str +
           '\u001b[' + inspect.colors[style][1] + 'm';
  } else {
    return str;
  }
}


function stylizeNoColor(str, styleType) {
  return str;
}


function arrayToHash(array) {
  var hash = {};

  array.forEach(function(val, idx) {
    hash[val] = true;
  });

  return hash;
}


function formatValue(ctx, value, recurseTimes) {
  // Provide a hook for user-specified inspect functions.
  // Check that value is an object with an inspect function on it
  if (ctx.customInspect &&
      value &&
      isFunction(value.inspect) &&
      // Filter out the util module, it's inspect function is special
      value.inspect !== exports.inspect &&
      // Also filter out any prototype objects using the circular check.
      !(value.constructor && value.constructor.prototype === value)) {
    var ret = value.inspect(recurseTimes, ctx);
    if (!isString(ret)) {
      ret = formatValue(ctx, ret, recurseTimes);
    }
    return ret;
  }

  // Primitive types cannot have properties
  var primitive = formatPrimitive(ctx, value);
  if (primitive) {
    return primitive;
  }

  // Look up the keys of the object.
  var keys = Object.keys(value);
  var visibleKeys = arrayToHash(keys);

  if (ctx.showHidden) {
    keys = Object.getOwnPropertyNames(value);
  }

  // IE doesn't make error fields non-enumerable
  // http://msdn.microsoft.com/en-us/library/ie/dww52sbt(v=vs.94).aspx
  if (isError(value)
      && (keys.indexOf('message') >= 0 || keys.indexOf('description') >= 0)) {
    return formatError(value);
  }

  // Some type of object without properties can be shortcutted.
  if (keys.length === 0) {
    if (isFunction(value)) {
      var name = value.name ? ': ' + value.name : '';
      return ctx.stylize('[Function' + name + ']', 'special');
    }
    if (isRegExp(value)) {
      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
    }
    if (isDate(value)) {
      return ctx.stylize(Date.prototype.toString.call(value), 'date');
    }
    if (isError(value)) {
      return formatError(value);
    }
  }

  var base = '', array = false, braces = ['{', '}'];

  // Make Array say that they are Array
  if (isArray(value)) {
    array = true;
    braces = ['[', ']'];
  }

  // Make functions say that they are functions
  if (isFunction(value)) {
    var n = value.name ? ': ' + value.name : '';
    base = ' [Function' + n + ']';
  }

  // Make RegExps say that they are RegExps
  if (isRegExp(value)) {
    base = ' ' + RegExp.prototype.toString.call(value);
  }

  // Make dates with properties first say the date
  if (isDate(value)) {
    base = ' ' + Date.prototype.toUTCString.call(value);
  }

  // Make error with message first say the error
  if (isError(value)) {
    base = ' ' + formatError(value);
  }

  if (keys.length === 0 && (!array || value.length == 0)) {
    return braces[0] + base + braces[1];
  }

  if (recurseTimes < 0) {
    if (isRegExp(value)) {
      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
    } else {
      return ctx.stylize('[Object]', 'special');
    }
  }

  ctx.seen.push(value);

  var output;
  if (array) {
    output = formatArray(ctx, value, recurseTimes, visibleKeys, keys);
  } else {
    output = keys.map(function(key) {
      return formatProperty(ctx, value, recurseTimes, visibleKeys, key, array);
    });
  }

  ctx.seen.pop();

  return reduceToSingleString(output, base, braces);
}


function formatPrimitive(ctx, value) {
  if (isUndefined(value))
    return ctx.stylize('undefined', 'undefined');
  if (isString(value)) {
    var simple = '\'' + JSON.stringify(value).replace(/^"|"$/g, '')
                                             .replace(/'/g, "\\'")
                                             .replace(/\\"/g, '"') + '\'';
    return ctx.stylize(simple, 'string');
  }
  if (isNumber(value))
    return ctx.stylize('' + value, 'number');
  if (isBoolean(value))
    return ctx.stylize('' + value, 'boolean');
  // For some reason typeof null is "object", so special case here.
  if (isNull(value))
    return ctx.stylize('null', 'null');
}


function formatError(value) {
  return '[' + Error.prototype.toString.call(value) + ']';
}


function formatArray(ctx, value, recurseTimes, visibleKeys, keys) {
  var output = [];
  for (var i = 0, l = value.length; i < l; ++i) {
    if (hasOwnProperty(value, String(i))) {
      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
          String(i), true));
    } else {
      output.push('');
    }
  }
  keys.forEach(function(key) {
    if (!key.match(/^\d+$/)) {
      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
          key, true));
    }
  });
  return output;
}


function formatProperty(ctx, value, recurseTimes, visibleKeys, key, array) {
  var name, str, desc;
  desc = Object.getOwnPropertyDescriptor(value, key) || { value: value[key] };
  if (desc.get) {
    if (desc.set) {
      str = ctx.stylize('[Getter/Setter]', 'special');
    } else {
      str = ctx.stylize('[Getter]', 'special');
    }
  } else {
    if (desc.set) {
      str = ctx.stylize('[Setter]', 'special');
    }
  }
  if (!hasOwnProperty(visibleKeys, key)) {
    name = '[' + key + ']';
  }
  if (!str) {
    if (ctx.seen.indexOf(desc.value) < 0) {
      if (isNull(recurseTimes)) {
        str = formatValue(ctx, desc.value, null);
      } else {
        str = formatValue(ctx, desc.value, recurseTimes - 1);
      }
      if (str.indexOf('\n') > -1) {
        if (array) {
          str = str.split('\n').map(function(line) {
            return '  ' + line;
          }).join('\n').substr(2);
        } else {
          str = '\n' + str.split('\n').map(function(line) {
            return '   ' + line;
          }).join('\n');
        }
      }
    } else {
      str = ctx.stylize('[Circular]', 'special');
    }
  }
  if (isUndefined(name)) {
    if (array && key.match(/^\d+$/)) {
      return str;
    }
    name = JSON.stringify('' + key);
    if (name.match(/^"([a-zA-Z_][a-zA-Z_0-9]*)"$/)) {
      name = name.substr(1, name.length - 2);
      name = ctx.stylize(name, 'name');
    } else {
      name = name.replace(/'/g, "\\'")
                 .replace(/\\"/g, '"')
                 .replace(/(^"|"$)/g, "'");
      name = ctx.stylize(name, 'string');
    }
  }

  return name + ': ' + str;
}


function reduceToSingleString(output, base, braces) {
  var numLinesEst = 0;
  var length = output.reduce(function(prev, cur) {
    numLinesEst++;
    if (cur.indexOf('\n') >= 0) numLinesEst++;
    return prev + cur.replace(/\u001b\[\d\d?m/g, '').length + 1;
  }, 0);

  if (length > 60) {
    return braces[0] +
           (base === '' ? '' : base + '\n ') +
           ' ' +
           output.join(',\n  ') +
           ' ' +
           braces[1];
  }

  return braces[0] + base + ' ' + output.join(', ') + ' ' + braces[1];
}


// NOTE: These type checking functions intentionally don't use `instanceof`
// because it is fragile and can be easily faked with `Object.create()`.
function isArray(ar) {
  return Array.isArray(ar);
}
exports.isArray = isArray;

function isBoolean(arg) {
  return typeof arg === 'boolean';
}
exports.isBoolean = isBoolean;

function isNull(arg) {
  return arg === null;
}
exports.isNull = isNull;

function isNullOrUndefined(arg) {
  return arg == null;
}
exports.isNullOrUndefined = isNullOrUndefined;

function isNumber(arg) {
  return typeof arg === 'number';
}
exports.isNumber = isNumber;

function isString(arg) {
  return typeof arg === 'string';
}
exports.isString = isString;

function isSymbol(arg) {
  return typeof arg === 'symbol';
}
exports.isSymbol = isSymbol;

function isUndefined(arg) {
  return arg === void 0;
}
exports.isUndefined = isUndefined;

function isRegExp(re) {
  return isObject(re) && objectToString(re) === '[object RegExp]';
}
exports.isRegExp = isRegExp;

function isObject(arg) {
  return typeof arg === 'object' && arg !== null;
}
exports.isObject = isObject;

function isDate(d) {
  return isObject(d) && objectToString(d) === '[object Date]';
}
exports.isDate = isDate;

function isError(e) {
  return isObject(e) &&
      (objectToString(e) === '[object Error]' || e instanceof Error);
}
exports.isError = isError;

function isFunction(arg) {
  return typeof arg === 'function';
}
exports.isFunction = isFunction;

function isPrimitive(arg) {
  return arg === null ||
         typeof arg === 'boolean' ||
         typeof arg === 'number' ||
         typeof arg === 'string' ||
         typeof arg === 'symbol' ||  // ES6 symbol
         typeof arg === 'undefined';
}
exports.isPrimitive = isPrimitive;

exports.isBuffer = require('./support/isBuffer');

function objectToString(o) {
  return Object.prototype.toString.call(o);
}


function pad(n) {
  return n < 10 ? '0' + n.toString(10) : n.toString(10);
}


var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
              'Oct', 'Nov', 'Dec'];

// 26 Feb 16:19:34
function timestamp() {
  var d = new Date();
  var time = [pad(d.getHours()),
              pad(d.getMinutes()),
              pad(d.getSeconds())].join(':');
  return [d.getDate(), months[d.getMonth()], time].join(' ');
}


// log is just a thin wrapper to console.log that prepends a timestamp
exports.log = function() {
  console.log('%s - %s', timestamp(), exports.format.apply(exports, arguments));
};


/**
 * Inherit the prototype methods from one constructor into another.
 *
 * The Function.prototype.inherits from lang.js rewritten as a standalone
 * function (not on Function.prototype). NOTE: If this file is to be loaded
 * during bootstrapping this function needs to be rewritten using some native
 * functions as prototype setup using normal JavaScript does not work as
 * expected during bootstrapping (see mirror.js in r114903).
 *
 * @param {function} ctor Constructor function which needs to inherit the
 *     prototype.
 * @param {function} superCtor Constructor function to inherit prototype from.
 */
exports.inherits = require('inherits');

exports._extend = function(origin, add) {
  // Don't do anything if add isn't an object
  if (!add || !isObject(add)) return origin;

  var keys = Object.keys(add);
  var i = keys.length;
  while (i--) {
    origin[keys[i]] = add[keys[i]];
  }
  return origin;
};

function hasOwnProperty(obj, prop) {
  return Object.prototype.hasOwnProperty.call(obj, prop);
}

}).call(this,require('_process'),typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{"./support/isBuffer":27,"_process":13,"inherits":8}],29:[function(require,module,exports){
/* jshint browserify: true */

'use strict';

/**
 * Optional entry point for browser builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-protocols')`.
 *
 */

var protocols = require('../../lib/protocols'),
    files = require('./lib/files'),
    schemas = require('../../lib/schemas'),
    types = require('../../lib/types'),
    values = require('../../lib/values');


function parse(schema, opts) {
  var obj = files.load(schema);
  return obj.protocol ?
    protocols.createProtocol(obj, opts) :
    types.createType(obj, opts);
}


module.exports = {
  Protocol: protocols.Protocol,
  Type: types.Type,
  assemble: schemas.assemble,
  combine: values.combine,
  infer: values.infer,
  parse: parse,
  types: types.builtins
};

},{"../../lib/protocols":32,"../../lib/schemas":33,"../../lib/types":34,"../../lib/values":36,"./lib/files":31}],30:[function(require,module,exports){
(function (Buffer){
/* jshint browserify: true */

'use strict';

/**
 * Shim to enable schema fingerprint computation.
 *
 * MD5 implementation originally from [1], used with permission from the
 * author, and lightly edited.
 *
 * [1] http://www.myersdaily.org/joseph/javascript/md5-text.html
 *
 */

function createHash(algorithm) {
  if (algorithm !== 'md5') {
    throw new Error('only md5 is supported in the browser');
  }
  return new Hash();
}

function Hash() { this.data = undefined; }
Hash.prototype.end = function (data) { this.data = data; };
Hash.prototype.read = function () { return md5(this.data); };

function md5cycle(x, k) {
  var a = x[0], b = x[1], c = x[2], d = x[3];

  a = ff(a, b, c, d, k[0], 7, -680876936);
  d = ff(d, a, b, c, k[1], 12, -389564586);
  c = ff(c, d, a, b, k[2], 17,  606105819);
  b = ff(b, c, d, a, k[3], 22, -1044525330);
  a = ff(a, b, c, d, k[4], 7, -176418897);
  d = ff(d, a, b, c, k[5], 12,  1200080426);
  c = ff(c, d, a, b, k[6], 17, -1473231341);
  b = ff(b, c, d, a, k[7], 22, -45705983);
  a = ff(a, b, c, d, k[8], 7,  1770035416);
  d = ff(d, a, b, c, k[9], 12, -1958414417);
  c = ff(c, d, a, b, k[10], 17, -42063);
  b = ff(b, c, d, a, k[11], 22, -1990404162);
  a = ff(a, b, c, d, k[12], 7,  1804603682);
  d = ff(d, a, b, c, k[13], 12, -40341101);
  c = ff(c, d, a, b, k[14], 17, -1502002290);
  b = ff(b, c, d, a, k[15], 22,  1236535329);

  a = gg(a, b, c, d, k[1], 5, -165796510);
  d = gg(d, a, b, c, k[6], 9, -1069501632);
  c = gg(c, d, a, b, k[11], 14,  643717713);
  b = gg(b, c, d, a, k[0], 20, -373897302);
  a = gg(a, b, c, d, k[5], 5, -701558691);
  d = gg(d, a, b, c, k[10], 9,  38016083);
  c = gg(c, d, a, b, k[15], 14, -660478335);
  b = gg(b, c, d, a, k[4], 20, -405537848);
  a = gg(a, b, c, d, k[9], 5,  568446438);
  d = gg(d, a, b, c, k[14], 9, -1019803690);
  c = gg(c, d, a, b, k[3], 14, -187363961);
  b = gg(b, c, d, a, k[8], 20,  1163531501);
  a = gg(a, b, c, d, k[13], 5, -1444681467);
  d = gg(d, a, b, c, k[2], 9, -51403784);
  c = gg(c, d, a, b, k[7], 14,  1735328473);
  b = gg(b, c, d, a, k[12], 20, -1926607734);

  a = hh(a, b, c, d, k[5], 4, -378558);
  d = hh(d, a, b, c, k[8], 11, -2022574463);
  c = hh(c, d, a, b, k[11], 16,  1839030562);
  b = hh(b, c, d, a, k[14], 23, -35309556);
  a = hh(a, b, c, d, k[1], 4, -1530992060);
  d = hh(d, a, b, c, k[4], 11,  1272893353);
  c = hh(c, d, a, b, k[7], 16, -155497632);
  b = hh(b, c, d, a, k[10], 23, -1094730640);
  a = hh(a, b, c, d, k[13], 4,  681279174);
  d = hh(d, a, b, c, k[0], 11, -358537222);
  c = hh(c, d, a, b, k[3], 16, -722521979);
  b = hh(b, c, d, a, k[6], 23,  76029189);
  a = hh(a, b, c, d, k[9], 4, -640364487);
  d = hh(d, a, b, c, k[12], 11, -421815835);
  c = hh(c, d, a, b, k[15], 16,  530742520);
  b = hh(b, c, d, a, k[2], 23, -995338651);

  a = ii(a, b, c, d, k[0], 6, -198630844);
  d = ii(d, a, b, c, k[7], 10,  1126891415);
  c = ii(c, d, a, b, k[14], 15, -1416354905);
  b = ii(b, c, d, a, k[5], 21, -57434055);
  a = ii(a, b, c, d, k[12], 6,  1700485571);
  d = ii(d, a, b, c, k[3], 10, -1894986606);
  c = ii(c, d, a, b, k[10], 15, -1051523);
  b = ii(b, c, d, a, k[1], 21, -2054922799);
  a = ii(a, b, c, d, k[8], 6,  1873313359);
  d = ii(d, a, b, c, k[15], 10, -30611744);
  c = ii(c, d, a, b, k[6], 15, -1560198380);
  b = ii(b, c, d, a, k[13], 21,  1309151649);
  a = ii(a, b, c, d, k[4], 6, -145523070);
  d = ii(d, a, b, c, k[11], 10, -1120210379);
  c = ii(c, d, a, b, k[2], 15,  718787259);
  b = ii(b, c, d, a, k[9], 21, -343485551);

  x[0] = add32(a, x[0]);
  x[1] = add32(b, x[1]);
  x[2] = add32(c, x[2]);
  x[3] = add32(d, x[3]);
}

function cmn(q, a, b, x, s, t) {
  a = add32(add32(a, q), add32(x, t));
  return add32((a << s) | (a >>> (32 - s)), b);
}

function ff(a, b, c, d, x, s, t) {
  return cmn((b & c) | ((~b) & d), a, b, x, s, t);
}

function gg(a, b, c, d, x, s, t) {
  return cmn((b & d) | (c & (~d)), a, b, x, s, t);
}

function hh(a, b, c, d, x, s, t) {
  return cmn(b ^ c ^ d, a, b, x, s, t);
}

function ii(a, b, c, d, x, s, t) {
  return cmn(c ^ (b | (~d)), a, b, x, s, t);
}

function md51(s) {
  var n = s.length,
  state = [1732584193, -271733879, -1732584194, 271733878], i;
  for (i=64; i<=s.length; i+=64) {
    md5cycle(state, md5blk(s.substring(i-64, i)));
  }

  s = s.substring(i-64);
  var tail = [0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0];
  for (i=0; i<s.length; i++) {
    tail[i>>2] |= s.charCodeAt(i) << ((i%4) << 3);
  }
  tail[i>>2] |= 0x80 << ((i%4) << 3);
  if (i > 55) {
    md5cycle(state, tail);
    for (i=0; i<16; i++) {
      tail[i] = 0;
    }
  }
  tail[14] = n*8;
  md5cycle(state, tail);
  return state;
}

function md5blk(s) {
  var md5blks = [], i;
  for (i=0; i<64; i+=4) {
    md5blks[i>>2] = s.charCodeAt(i) +
      (s.charCodeAt(i+1) << 8) +
      (s.charCodeAt(i+2) << 16) +
      (s.charCodeAt(i+3) << 24);
  }
  return md5blks;
}

function md5(s) {
  var arr = md51(s);
  var buf = new Buffer(16);
  var i;
  for (i = 0; i < 4; i++) {
    buf.writeIntLE(arr[i], i * 4, 4);
  }
  return buf;
}

function add32(a, b) {
  return (a + b) & 0xFFFFFFFF;
}

module.exports = {
  createHash: createHash
};

}).call(this,require("buffer").Buffer)
},{"buffer":4}],31:[function(require,module,exports){
(function (Buffer){
/* jshint node: true */

'use strict';

/**
 * Shim without file-system operations.
 *
 * We also patch a few methods because of browser incompatibilities (see below
 * for more information).
 *
 */

// Since there are no utf8 and binary functions on browserify's `Buffer`, we
// must patch in tap methods using the generic slice and write methods.
(function polyfillTap() {
  var Tap = require('../../../lib/utils').Tap;

  Tap.prototype.readString = function () {
    var len = this.readLong();
    var pos = this.pos;
    var buf = this.buf;
    this.pos += len;
    if (this.pos > buf.length) {
      return;
    }
    return this.buf.slice(pos, pos + len).toString();
  };

  Tap.prototype.writeString = function (s) {
    var len = Buffer.byteLength(s);
    this.writeLong(len);
    var pos = this.pos;
    this.pos += len;
    if (this.pos > this.buf.length) {
      return;
    }
    this.buf.write(s, pos);
  };

  Tap.prototype.writeBinary = function (s, len) {
    var pos = this.pos;
    this.pos += len;
    if (this.pos > this.buf.length) {
      return;
    }
    this.buf.write(s, pos, len, 'binary');
  };
})();


/**
 * Schema loader, without file-system access.
 *
 */
function load(schema) {
  var obj;
  if (typeof schema == 'string' && schema !== 'null') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      // No file loading here.
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return obj;
}

/**
 * Default (erroring) hook for assembling IDLs.
 *
 */
function createImportHook() {
  return function (fpath, kind, cb) { cb(new Error('missing import hook')); };
}


module.exports = {
  createImportHook: createImportHook,
  load: load
};

}).call(this,require("buffer").Buffer)
},{"../../../lib/utils":35,"buffer":4}],32:[function(require,module,exports){
(function (process,Buffer){
/* jshint node: true */

// TODO: Explore making `MessageEmitter` a writable stream, and
// `MessageListener` a readable stream. The main inconsistency is w.r.t.
// watermarks (the standard stream behavior doesn't support waiting for the
// callbacks, without also preventing concurrent requests).
// TODO: See whether it is worth it to remove listeners from readable and
// writable streams when stateless emitters and listeners are destroyed.
// TODO: Add protocol "discover" method?

'use strict';

/**
 * This module implements Avro's IPC/RPC logic.
 *
 * This is done the Node.js way, mimicking the `EventEmitter` class.
 *
 */

var types = require('./types'),
    utils = require('./utils'),
    events = require('events'),
    stream = require('stream'),
    util = require('util');


// Various useful types. We instantiate options once, to share the registry.

var OPTS = {};

var BOOLEAN_TYPE = types.createType('boolean', OPTS);
var MAP_BYTES_TYPE = types.createType({type: 'map', values: 'bytes'}, OPTS);
var STRING_TYPE = types.createType('string', OPTS);

var HANDSHAKE_REQUEST_TYPE = types.createType({
  namespace: 'org.apache.avro.ipc',
  name: 'HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', MAP_BYTES_TYPE], 'default': null}
  ]
}, OPTS);

var HANDSHAKE_RESPONSE_TYPE = types.createType({
  namespace: 'org.apache.avro.ipc',
  name: 'HandshakeResponse',
  type: 'record',
  fields: [
    {
      name: 'match',
      type: {
        name: 'HandshakeMatch',
        type: 'enum',
        symbols: ['BOTH', 'CLIENT', 'NONE']
      }
    },
    {name: 'serverProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: ['null', 'MD5'], 'default': null},
    {name: 'meta', type: ['null', MAP_BYTES_TYPE], 'default': null}
  ]
}, OPTS);

// A few convenience imports.

var Tap = utils.Tap;
var f = util.format;

/**
 * Protocol generation function.
 *
 * This should be used instead of the protocol constructor. The protocol's
 * constructor performs no logic to better support efficient protocol copy.
 *
 */
function createProtocol(attrs, opts) {
  opts = opts || {};
  var name = attrs.protocol;
  if (!name) {
    throw new Error('missing protocol name');
  }
  if (attrs.namespace !== undefined) {
    opts.namespace = attrs.namespace;
  }
  if (opts.namespace && !~name.indexOf('.')) {
    name = f('%s.%s', opts.namespace, name);
  }
  if (attrs.types) {
    attrs.types.forEach(function (obj) { types.createType(obj, opts); });
  }
  var messages = {};
  if (attrs.messages) {
    Object.keys(attrs.messages).forEach(function (key) {
      messages[key] = new Message(key, attrs.messages[key], opts);
    });
  }
  return new Protocol(name, messages, opts.registry || {});
}

/**
 * An Avro protocol.
 *
 */
function Protocol(name, messages, types, handlers) {
  if (types === undefined) {
    // Let's be helpful in case this class is instantiated directly.
    return createProtocol(name, messages);
  }

  this._name = name;
  this._messages = messages;
  this._types = types;
  // Shared with subprotocols (via the prototype chain, overwriting is safe).
  this._handlers = handlers || {};
  // We cache a string rather than a buffer to not retain an entire slab. This
  // also lets us more use hashes as keys inside maps (e.g. for resolvers).
  this._hs = utils.getHash(this.getSchema()).toString('binary');
}

Protocol.prototype.subprotocol = function () {
  // Return a copy of the protocol, but a separate namespace for handlers which
  // inherits from the parent protocol. This can be useful for organizing
  // protocols when there are many handlers.
  return new Protocol(
    this._name,
    this._messages,
    this._types,
    Object.create(this._handlers)
  );
};

Protocol.prototype.createEmitter = function (transport, opts) {
  var objectMode = opts && opts.objectMode;
  if (typeof transport == 'function') {
    var writableFactory;
    if (objectMode) {
      writableFactory = transport;
    } else {
      // We provide a default standard-compliant codec. This should support
      // most use-cases (for example when speaking to the official Java and
      // Python implementations over HTTP, or when this library is used for
      // both the emitting and listening sides).
      writableFactory = function (cb) {
        var encoder = new FrameEncoder(opts);
        encoder.pipe(transport(function (err, readable) {
          if (err) {
            cb(err);
            return;
          }
          cb(null, readable.pipe(new FrameDecoder()));
        }));
        return encoder;
      };
    }
    return new StatelessEmitter(this, writableFactory, opts);
  } else {
    var readable, writable;
    if (isStream(transport)) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    if (!objectMode) {
      // To ease communication with Java servers, we provide a non-standard
      // default codec here (but compatible with Java servers'
      // `NettyTransportCodec`'s implementation). This is unfortunate but
      // probably a good compromise in practice.
      readable = readable.pipe(new NettyDecoder());
      var encoder = new NettyEncoder();
      encoder.pipe(writable);
      writable = encoder;
    }
    return new StatefulEmitter(this, readable, writable, opts);
  }
};

Protocol.prototype.createListener = function (transport, opts) {
  // See `createEmitter` for `objectMode` motivations.
  var objectMode = opts && opts.objectMode;
  if (typeof transport == 'function') {
    var readableFactory;
    if (objectMode) {
      readableFactory = transport;
    } else {
      readableFactory = function (cb) {
        return transport(function (err, writable) {
          if (err) {
            cb(err);
            return;
          }
          var encoder = new FrameEncoder(opts);
          encoder.pipe(writable);
          cb(null, encoder);
        }).pipe(new FrameDecoder());
      };
    }
    return new StatelessListener(this, readableFactory, opts);
  } else {
    var readable, writable;
    if (isStream(transport)) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    if (!objectMode) {
      readable = readable.pipe(new NettyDecoder());
      var encoder = new NettyEncoder();
      encoder.pipe(writable);
      writable = encoder;
    }
    return new StatefulListener(this, readable, writable, opts);
  }
};

Protocol.prototype.emit = function (name, req, emitter, cb) {
  if (!emitter || !this.equals(emitter.getProtocol())) {
    throw new Error('invalid emitter');
  }
  var message = this._messages[name];
  if (!message) {
    throw new Error('unknown message: ' + name);
  }
  var self = this;
  emitter.emitMessage(name, {request: req}, function (err, resEnv) {
    var errType = message.getErrorType();
    // System error, likely the message wasn't sent (or an error occurred while
    // decoding the response).
    if (err) {
      if (this._strict) {
        err = errType.clone(err.message, {wrapUnions: true});
      }
      done(err);
      return;
    }
    // Message transmission succeeded, we transmit the message data; massaging
    // any error strings into actual `Error` objects in non-strict mode.
    err = resEnv.error;
    if (!this._strict) {
      if (err === undefined) {
        err = null;
      } else if (types.Type.isType(errType, 'union:unwrapped')) {
        if (typeof err == 'string') {
          err = new Error(err);
        }
      } else if (err && err.string) {
        err = new Error(err.string);
      }
    }
    done(err, resEnv.response);
  });
  return emitter.getPending();

  function done(err, res) {
    if (cb) {
      cb.call(self, err, res);
    } else if (err) {
      emitter.emit('error', err);
    }
  }
};

Protocol.prototype.on = function (name, handler) {
  if (!this._messages[name]) {
    throw new Error(f('unknown message: %s', name));
  }
  this._handlers[name] = handler;
  return this;
};

Protocol.prototype.getHandler = function (name) {
  return this._handlers[name];
};

Protocol.prototype.getName = function () { return this._name; };

Protocol.prototype.getType = function (name) { return this._types[name]; };

Protocol.prototype.getMessage = function (name) {
  return this._messages[name];
};

Protocol.prototype.getMessages = function () {
  var messages = this._messages;
  return Object.keys(messages).map(function (name) { return messages[name]; });
};

Protocol.prototype.getSchema = function (opts) {
  var namedTypes = [];
  Object.keys(this._types).forEach(function (name) {
    var type = this._types[name];
    if (type.getName()) {
      namedTypes.push(type);
    }
  }, this);
  return types.stringify({
    protocol: this._name,
    types: namedTypes.length ? namedTypes : undefined,
    messages: Object.keys(this._messages).length ? this._messages : undefined
  }, opts);
};

Protocol.prototype.getFingerprint = function (algorithm) {
  if (!algorithm) {
    // We can use the cached hash.
    return new Buffer(this._hs, 'binary');
  } else {
    return utils.getHash(this.getSchema());
  }
};

Protocol.prototype.equals = function (ptcl) {
  return !!ptcl && this._hs === ptcl._hs;
};

Protocol.prototype.toString = function () {
  return this.getSchema({noDeref: true});
};

Protocol.prototype.inspect = function () {
  return f('<Protocol %j>', this._name);
};

Protocol.MessageEmitter = MessageEmitter;
Protocol.MessageListener = MessageListener;

/**
 * Base message emitter class.
 *
 * See below for the two available variants.
 *
 */
function MessageEmitter(ptcl, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);
  this._ptcl = ptcl;
  this._strict = !!opts.strictErrors;

  this._timeout = opts.timeout === undefined ? 10000 : opts.timeout;
  this._cache = opts.cache || {};
  var fgpt = opts.serverFingerprint;
  var adapter;
  if (fgpt) {
    adapter = this._cache[fgpt];
  }
  if (!adapter) {
    // This might happen even if the server fingerprint option was set, in
    // cases where the cache doesn't contain the corresponding adapter.
    fgpt = ptcl.getFingerprint();
    adapter = this._cache[fgpt] = new Adapter(ptcl, ptcl, fgpt);
  }
  this._adapter = adapter;

  this._registry = new Registry(this);
  this._destroyed = false;
  this._interrupted = false;
  this.once('_eot', function (pending) { this.emit('eot', pending); });
}
util.inherits(MessageEmitter, events.EventEmitter);

MessageEmitter.prototype.getCache = function () { return this._cache; };

MessageEmitter.prototype.getProtocol = function () { return this._ptcl; };

MessageEmitter.prototype.getTimeout = function () { return this._timeout; };

MessageEmitter.prototype.isDestroyed = function () { return this._destroyed; };

MessageEmitter.prototype.getPending = function () {
  return this._registry.size();
};

MessageEmitter.prototype.emitMessage = function (name, reqEnv, opts, cb) {
  if (cb === undefined && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }
  if (!cb) {
    throw new Error('missing callback');
  }

  // Serialize the message.
  var err, msg, reqBuf;
  if (this._destroyed) {
    err = new Error('destroyed');
  } else if (name === '') {
    // This is a ping request.
    reqBuf = new Buffer([0, 0]); // No header, empty message name.
  } else {
    msg = this._ptcl.getMessage(name);
    if (!msg) {
      err = new Error('missing message');
    } else {
      try {
        reqBuf = Buffer.concat([
          MAP_BYTES_TYPE.toBuffer(reqEnv.header || {}),
          STRING_TYPE.toBuffer(name),
          msg.getRequestType().toBuffer(reqEnv.request)
        ]);
      } catch (cause) {
        err = wrapError('invalid request', cause);
      }
    }
  }

  // Return now if a serialization error occurred.
  var self = this;
  if (err) {
    process.nextTick(function () { cb.call(self, err); });
    return true;
  }

  // Generate the response callback.
  var timeout = (opts && opts.timeout !== undefined) ?
    opts.timeout :
    this._timeout;
  var id = this._registry.add(timeout, function (err, resBuf, adapter) {
    var resEnv;
    if (!err) {
      if (name === '') {
        resEnv = {};
      } else {
        try {
          resEnv = adapter.decodeResponse(resBuf, name).envelope;
        } catch (cause) {
          err = wrapError('invalid response', cause);
        }
      }
    }
    var meta;
    if (adapter) {
      meta = {
        serverFingerprint: adapter._fingerprint,
        serverProtocol: adapter.getServerProtocol()
      };
    }
    cb.call(this, err, resEnv, meta);
    if (this._destroyed && !this._interrupted && !this._registry.size()) {
      this.destroy();
    }
  });

  return this._send(id, reqBuf, !!msg && msg.isOneWay());
};

MessageEmitter.prototype.destroy = function (noWait) {
  this._destroyed = true;
  var registry = this._registry;
  var pending = registry.size();
  if (noWait && pending) {
    this._interrupted = true;
    registry.clear();
  }
  if (noWait || !pending) {
    this.emit('_eot', pending);
  }
};

MessageEmitter.prototype._send = utils.abstractFunction;

MessageEmitter.prototype._createHandshakeRequest = function (adapter, noPtcl) {
  var ptcl = this._ptcl;
  return {
    clientHash: ptcl.getFingerprint(),
    clientProtocol: noPtcl ? null : ptcl.getSchema({exportAttrs: true}),
    serverHash: adapter._fingerprint
  };
};

MessageEmitter.prototype._getAdapter = function (hres) {
  var serverBuf = hres.serverHash;
  var adapter = this._cache[serverBuf];
  if (adapter) {
    return adapter;
  }
  var serverPtcl = createProtocol(JSON.parse(hres.serverProtocol));
  adapter = new Adapter(this._ptcl, serverPtcl, serverBuf);
  return this._cache[serverBuf] = adapter;
};

/**
 * Factory-based emitter.
 *
 * This emitter doesn't keep a persistent connection to the server and requires
 * prepending a handshake to each message emitted. Usage examples include
 * talking to an HTTP server (where the factory returns an HTTP request).
 *
 * Since each message will use its own writable/readable stream pair, the
 * advantage of this emitter is that it is able to keep track of which response
 * corresponds to each request without relying on transport ordering. In
 * particular, this means these emitters are compatible with any server
 * implementation.
 *
 */
function StatelessEmitter(ptcl, writableFactory, opts) {
  MessageEmitter.call(this, ptcl, opts);
  this._writableFactory = writableFactory;

  if (!opts || !opts.noPing) {
    // Ping the server to check whether the remote protocol is compatible.
    this.emitMessage('', {}, function (err) {
      if (err) {
        this.emit('error', err);
      }
    });
  }
}
util.inherits(StatelessEmitter, MessageEmitter);

StatelessEmitter.prototype._send = function (id, reqBuf) {
  var cb = this._registry.get(id);
  var adapter = this._adapter;
  var self = this;
  process.nextTick(emit);
  return true; // Each writable is only used once, no risk of buffering.

  function emit(retry) {
    var hreq = self._createHandshakeRequest(adapter, !retry);

    var writable = self._writableFactory.call(self, function (err, readable) {
      if (err) {
        cb(err);
        return;
      }
      readable.on('data', function (obj) {
        var buf = Buffer.concat(obj.payload);
        try {
          var parts = readHead(HANDSHAKE_RESPONSE_TYPE, buf);
          var hres = parts.head;
          if (hres.serverHash) {
            adapter = self._getAdapter(hres);
          }
          self.emit('handshake', hreq, hres);
          if (hres.match === 'NONE') {
            emit(true);
            return;
          }
          // Change the default adapter.
          self._adapter = adapter;
        } catch (err) {
          cb(err);
          return;
        }
        cb(null, parts.tail, adapter);
      });
    });

    writable.end({
      id: id,
      payload: [HANDSHAKE_REQUEST_TYPE.toBuffer(hreq), reqBuf]
    });
  }
};

/**
 * Multiplexing emitter.
 *
 * These emitters reuse the same streams (both readable and writable) for all
 * messages. This avoids a lot of overhead (e.g. creating new connections,
 * re-issuing handshakes) but requires the underlying transport to support
 * forwarding message IDs.
 *
 */
function StatefulEmitter(ptcl, readable, writable, opts) {
  MessageEmitter.call(this, ptcl, opts);
  this._readable = readable;
  this._writable = writable;
  this._connected = !!(opts && opts.noPing);
  this._readable.on('end', function () { self.destroy(true); });
  this._writable.on('finish', function () { self.destroy(); });

  this.on('eot', function () {
    // Remove references to this emitter to avoid potential memory leaks.
    this._readable
      .removeListener('data', onPing)
      .removeListener('data', onMessage);
  });

  var self = this;
  var hreq; // For handshake events.
  if (this._connected) {
    this._readable.on('data', onMessage);
  } else {
    this._readable.on('data', onPing);
    process.nextTick(ping);
  }

  function ping(retry) {
    if (self._destroyed) {
      return;
    }
    hreq = self._createHandshakeRequest(self._adapter, !retry);
    var payload = [
      HANDSHAKE_REQUEST_TYPE.toBuffer(hreq),
      new Buffer([0, 0]) // No header, no data (empty message name).
    ];
    self._writable.write({id: 0, payload: payload});
  }

  function onPing(obj) {
    var buf = Buffer.concat(obj.payload);
    try {
      var hres = readHead(HANDSHAKE_RESPONSE_TYPE, buf).head;
      if (hres.serverHash) {
        self._adapter = self._getAdapter(hres);
      }
    } catch (err) {
      self.destroy(true); // Not a recoverable error.
      self.emit('error', wrapError('handshake error', err));
      return;
    }
    self.emit('handshake', hreq, hres);
    if (hres.match === 'NONE') {
      ping(true);
    } else {
      self._readable.removeListener('data', onPing).on('data', onMessage);
      self._connected = true;
      self.emit('_connected');
      hreq = null; // Release reference.
    }
  }

  // Callback used after a connection has been established.
  function onMessage(obj) {
    var cb = self._registry.get(obj.id);
    if (cb) {
      process.nextTick(function () {
        // Ensure that the initial callback gets called asynchronously, even
        // for completely synchronous transports (otherwise the number of
        // pending requests will sometimes be inconsistent between stateful and
        // stateless transports).
        cb(null, Buffer.concat(obj.payload), self._adapter);
      });
    }
  }
}
util.inherits(StatefulEmitter, MessageEmitter);

StatefulEmitter.prototype._send = function (id, reqBuf, isOneWay) {
  if (!this._connected) {
    this.once('_connected', function () { this._send(id, reqBuf, isOneWay); });
    return false; // Call is being buffered.
  }
  if (isOneWay) {
    var self = this;
    // Clear the callback, passing in an empty header.
    process.nextTick(function () {
      self._registry.get(id)(null, new Buffer([0, 0, 0]), self._adapter);
    });
  }
  return this._writable.write({id: id, payload: [reqBuf]});
};

/**
 * The server-side emitter equivalent.
 *
 */
function MessageListener(ptcl, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);
  this._ptcl = ptcl;
  this._strict = !!opts.strictErrors;
  this._cache = opts.cache || {};

  var fgpt = this._ptcl.getFingerprint();
  if (!this._cache[fgpt]) {
    // Add the listener's protocol to the cache if it isn't already there. This
    // will save a handshake the first time on emitters with the same protocol.
    this._cache[fgpt] = new Adapter(this._ptcl, this._ptcl, fgpt);
  }

  this._adapter = null;
  this._hook = null;

  this._pending = 0;
  this._destroyed = false;
  this._interrupted = false;
  this.once('_eot', function (pending) { this.emit('eot', pending); });
}
util.inherits(MessageListener, events.EventEmitter);

MessageListener.prototype.getCache = function () { return this._cache; };

MessageListener.prototype.getPending = function () { return this._pending; };

MessageListener.prototype.getProtocol = function () { return this._ptcl; };

MessageListener.prototype.isDestroyed = function () {
  return this._destroyed;
};

MessageListener.prototype.onMessage = function (fn) {
  this._hook = fn;
  return this;
};

MessageListener.prototype.destroy = function (noWait) {
  this._destroyed = true;
  if (noWait || !this._pending) {
    this._interrupted = true;
    this.emit('_eot', this._pending);
  }
};

MessageListener.prototype._receive = function (reqBuf, adapter, cb) {
  var ptcl = this._ptcl;
  var self = this;
  try {
    var decoded = adapter.decodeRequest(reqBuf);
  } catch (err) {
    cb(encodeError(err));
    return;
  }

  var clientMsg = decoded.message;
  if (!clientMsg) {
    // Ping request, return an empty response.
    cb(new Buffer(0));
    return;
  }
  var name = clientMsg.getName();
  var serverMsg = ptcl.getMessage(name);
  var handler = ptcl._handlers[name];
  var reqEnv = decoded.envelope;
  this._pending++;
  if (this._hook) {
    // Custom hook.
    var meta = {
      clientFingerprint: adapter._fingerprint,
      clientProtocol: adapter.getClientProtocol()
    };
    this._hook.call(this, name, reqEnv, meta, done);
  } else if (handler) {
    try {
      if (serverMsg.isOneWay()) {
        handler.call(ptcl, reqEnv.request);
        done(null, null);
      } else {
        handler.call(ptcl, reqEnv.request, this, function (err, res) {
          var errType = serverMsg.getErrorType();
          if (!self._strict) {
            if (isError(err)) {
              err = errType.clone(err.message, {wrapUnions: true});
            } else if (err === null) {
              err = undefined;
            }
          }
          done(null, {error: err, response: res});
        });
      }
    } catch (err) {
      // We catch synchronous failures (same as express) and return the
      // failure. Note that the server process can still crash if an error is
      // thrown after the handler returns but before the response is sent
      // (again, same as express).
      done(err);
    }
  } else {
    // The underlying protocol hasn't implemented a handler for this message.
    done(new Error(f('unhandled message: %s', name)));
  }

  function done(err, resEnv) {
    self._pending--;
    var resBuf;
    if (!err) {
      var errType = serverMsg.getErrorType();
      var noError = resEnv.error === undefined;
      try {
        var header = MAP_BYTES_TYPE.toBuffer(resEnv.header || {});
        resBuf = Buffer.concat([
          header,
          BOOLEAN_TYPE.toBuffer(!noError),
          noError ?
            serverMsg.getResponseType().toBuffer(resEnv.response) :
            errType.toBuffer(resEnv.error)
        ]);
      } catch (cause) {
        err = wrapError('invalid response', cause);
      }
    }
    if (err) {
      resBuf = encodeError(err, header);
    }
    if (!self._interrupted) {
      cb(resBuf, serverMsg.isOneWay());
    }
    if (self._destroyed && !self._pending) {
      self.destroy();
    }
  }
};

MessageListener.prototype._createHandshakeResponse = function (err, hreq) {
  var ptcl = this._ptcl;
  var buf = ptcl.getFingerprint();
  var serverMatch = hreq && hreq.serverHash.equals(buf);
  return {
    match: err ? 'NONE' : (serverMatch ? 'BOTH' : 'CLIENT'),
    serverProtocol: serverMatch ? null : ptcl.getSchema({exportAttrs: true}),
    serverHash: serverMatch ? null : buf
  };
};

MessageListener.prototype._getAdapter = function (hreq) {
  var clientBuf = hreq.clientHash;
  var adapter = this._cache[clientBuf];
  if (adapter) {
    return adapter;
  }
  if (!hreq.clientProtocol) {
    throw new Error('unknown protocol');
  }
  var clientPtcl = createProtocol(JSON.parse(hreq.clientProtocol));
  adapter = new Adapter(clientPtcl, this._ptcl, clientBuf);
  return this._cache[clientBuf] = adapter;
};

/**
 * MessageListener for stateless transport.
 *
 * This listener expect a handshake to precede each message.
 *
 */
function StatelessListener(ptcl, readableFactory, opts) {
  MessageListener.call(this, ptcl, opts);
  var self = this;

  process.nextTick(function () {
    // Delay listening to allow handlers to be attached even if the factory is
    // purely synchronous.
    readableFactory.call(this, function (err, writable) {
      if (err) {
        onFinish();
        return;
      }
      self._writable = writable.on('finish', onFinish);
      self.emit('_writable');
    }).once('data', onRequest)
      .on('end', function() { self.destroy(); });
  });

  function onRequest(obj) {
    var id = obj.id;
    var buf = Buffer.concat(obj.payload);
    var err = null;
    try {
      var parts = readHead(HANDSHAKE_REQUEST_TYPE, buf);
      var hreq = parts.head;
      var adapter = self._getAdapter(hreq);
    } catch (cause) {
      err = wrapError('invalid handshake request', cause);
    }

    if (err) {
      done(encodeError(err));
    } else {
      self._receive(parts.tail, adapter, done);
    }

    function done(resBuf) {
      var hres = self._createHandshakeResponse(err, hreq);
      self.emit('handshake', hreq, hres);
      if (self._writable) {
        var payload = [
          HANDSHAKE_RESPONSE_TYPE.toBuffer(hres),
          resBuf
        ];
        self._writable.end({id: id, payload: payload});
      } else {
        self.once('_writable', function () { done(resBuf); });
      }
    }
  }

  function onFinish() { self.destroy(true); }
}
util.inherits(StatelessListener, MessageListener);

/**
 * Stateful transport listener.
 *
 * A handshake is done when the listener is first opened, then all messages are
 * sent without.
 *
 */
function StatefulListener(ptcl, readable, writable, opts) {
  MessageListener.call(this, ptcl, opts);
  this._adapter = undefined;
  this._writable = writable.on('finish', onFinish);
  this._readable = readable.on('data', onHandshake).on('end', onEnd);

  this.on('eot', function () {
    // Clean up any references to the listener on the underlying streams.
    this._writable.removeListener('finish', onFinish);
    this._readable
      .removeListener('data', onHandshake)
      .removeListener('data', onRequest)
      .removeListener('end', onEnd);
  });

  var self = this;

  function onHandshake(obj) {
    var id = obj.id;
    var buf = Buffer.concat(obj.payload);
    var err;
    try {
      var parts = readHead(HANDSHAKE_REQUEST_TYPE, buf);
      var hreq = parts.head;
      self._adapter = self._getAdapter(hreq);
    } catch (cause) {
      err = wrapError('invalid handshake request', cause);
    }
    if (err) {
      // Either the client's protocol was unknown or it isn't compatible.
      done(encodeError(err));
    } else {
      self._readable
        .removeListener('data', onHandshake)
        .on('data', onRequest);
      self._receive(parts.tail, self._adapter, done);
    }

    function done(resBuf) {
      var hres = self._createHandshakeResponse(err, hreq);
      self.emit('handshake', hreq, hres);
      var payload = [
        HANDSHAKE_RESPONSE_TYPE.toBuffer(hres),
        resBuf
      ];
      self._writable.write({id: id, payload: payload});
    }
  }

  function onRequest(obj) {
    // These requests are not prefixed with handshakes.
    var id = obj.id;
    var reqBuf = Buffer.concat(obj.payload);
    self._receive(reqBuf, self._adapter, function (resBuf, isOneWay) {
      if (!isOneWay) {
        self._writable.write({id: id, payload: [resBuf]});
      }
    });
  }

  function onEnd() { self.destroy(); }

  function onFinish() { self.destroy(true); }
}
util.inherits(StatefulListener, MessageListener);

/**
 * An Avro message.
 *
 * It contains the various types used to send it (request, error, response).
 *
 */
function Message(name, attrs, opts) {
  opts = opts || {};
  this._name = name;

  var requestName = 'org.apache.avro.ipc.Request'; // Placeholder name.
  this._requestType = types.createType({
    type: 'record',
    name: requestName,
    namespace: opts.namespace || '',
    fields: attrs.request
  }, opts);
  delete opts.registry[requestName];

  if (!attrs.response) {
    throw new Error('missing response');
  }
  this._responseType = types.createType(attrs.response, opts);

  var errors = attrs.errors || [];
  errors.unshift('string');
  this._errorType = types.createType(errors, opts);

  this._oneWay = !!attrs['one-way'];
  if (this._oneWay) {
    if (this._responseType.getTypeName() !== 'null' || errors.length > 1) {
      throw new Error('unapplicable one-way parameter');
    }
  }
}

Message.prototype.getName = function () { return this._name; };

Message.prototype.getRequestType = function () { return this._requestType; };

Message.prototype.getResponseType = function () { return this._responseType; };

Message.prototype.getErrorType = function () { return this._errorType; };

Message.prototype.isOneWay = function () { return this._oneWay; };

Message.prototype.inspect = Message.prototype.toJSON = function () {
  var obj = {
    request: this._requestType.getFields(),
    response: this._responseType
  };
  var errorTypes = this._errorType.getTypes();
  if (errorTypes.length > 1) {
    obj.errors = types.createType(errorTypes.slice(1));
  }
  if (this._oneWay) {
    obj['one-way'] = true;
  }
  return obj;
};

// Helpers.

/**
 * Callback registry.
 *
 * Callbacks added must accept an error as first argument. This is used by
 * message emitters to store pending calls.
 *
 */
function Registry(ctx) {
  this._ctx = ctx; // Context for all callbacks.
  this._id = 0; // Unique integer ID for each call.
  this._n = 0; // Number of pending calls.
  this._cbs = {};
}

Registry.prototype.size = function () { return this._n; };

Registry.prototype.get = function (id) { return this._cbs[id]; };

Registry.prototype.add = function (timeout, fn) {
  this._id = (this._id + 1) | 0;

  var self = this;
  var id = this._id;
  var timer;
  if (timeout > 0) {
    timer = setTimeout(function () { cb(new Error('timeout')); }, timeout);
  }

  this._cbs[id] = cb;
  this._n++;
  return id;

  function cb() {
    if (!self._cbs[id]) {
      // The callback has already run.
      return;
    }
    delete self._cbs[id];
    self._n--;
    if (timer) {
      clearTimeout(timer);
    }
    fn.apply(self._ctx, arguments);
  }
};

Registry.prototype.clear = function () {
  Object.keys(this._cbs).forEach(function (id) {
    this._cbs[id](new Error('interrupted'));
  }, this);
};

/**
 * Protocol resolution helper.
 *
 * It is used both by emitters and listeners, to respectively decode errors and
 * responses, or requests.
 *
 */
function Adapter(clientPtcl, serverPtcl, fingerprint) {
  this._clientPtcl = clientPtcl;
  this._serverPtcl = serverPtcl;
  this._fingerprint = fingerprint; // Convenience.
  this._rsvs = clientPtcl.equals(serverPtcl) ? null : this._createResolvers();
}

Adapter.prototype.getClientProtocol = function () { return this._clientPtcl; };

Adapter.prototype.getServerProtocol = function () { return this._serverPtcl; };

Adapter.prototype._createResolvers = function () {
  var rsvs = {};
  this._clientPtcl.getMessages().forEach(function (c) {
    var n = c.getName();
    var s = this._serverPtcl.getMessage(n);
    if (!s) {
      throw new Error(f('missing server message: %s', n));
    }
    if (s.isOneWay() !== c.isOneWay()) {
      throw new Error(f('inconsistent one-way parameter for message: %s', n));
    }
    try {
      rsvs[n + '?'] = s.getRequestType().createResolver(c.getRequestType());
      rsvs[n + '*'] = c.getErrorType().createResolver(s.getErrorType());
      rsvs[n + '!'] = c.getResponseType().createResolver(s.getResponseType());
    } catch (err) {
      throw wrapError('incompatible message ' + n, err);
    }
  }, this);
  return rsvs;
};

Adapter.prototype._getReader = function (name, qualifier) {
  if (this._rsvs) {
    return this._rsvs[name + qualifier];
  } else {
    var msg = this._serverPtcl.getMessage(name);
    switch (qualifier) {
      case '?': return msg.getRequestType();
      case '*': return msg.getErrorType();
      case '!': return msg.getResponseType();
    }
  }
};

Adapter.prototype.decodeRequest = function (buf) {
  var tap = new Tap(buf);
  var hdr = MAP_BYTES_TYPE._read(tap);
  var name = STRING_TYPE._read(tap);
  if (name) {
    var req = this._getReader(name, '?')._read(tap);
  }
  if (!tap.isValid()) {
    throw new Error('truncated request');
  }
  return {
    message: this._clientPtcl.getMessage(name),
    envelope: {header: hdr, request: req}
  };
};

Adapter.prototype.decodeResponse = function (buf, name) {
  var tap = new Tap(buf);
  var hdr = MAP_BYTES_TYPE._read(tap);
  var isError = BOOLEAN_TYPE._read(tap);
  var reader = this._getReader(name, isError ? '*' : '!');
  if (isError) {
    var err = reader._read(tap);
  } else {
    var res = reader._read(tap);
  }
  if (!tap.isValid()) {
    throw new Error('truncated response');
  }
  return {
    message: this._serverPtcl.getMessage(name),
    envelope: {header: hdr, error: err, response: res}
  };
};

/**
 * Standard "un-framing" stream.
 *
 */
function FrameDecoder() {
  stream.Transform.call(this, {readableObjectMode: true});
  this._id = undefined;
  this._buf = new Buffer(0);
  this._bufs = [];

  this.on('finish', function () { this.push(null); });
}
util.inherits(FrameDecoder, stream.Transform);

FrameDecoder.prototype._transform = function (buf, encoding, cb) {
  buf = Buffer.concat([this._buf, buf]);
  var frameLength;
  while (
    buf.length >= 4 &&
    buf.length >= (frameLength = buf.readInt32BE(0)) + 4
  ) {
    if (frameLength) {
      this._bufs.push(buf.slice(4, frameLength + 4));
    } else {
      var bufs = this._bufs;
      this._bufs = [];
      this.push({id: null, payload: bufs});
    }
    buf = buf.slice(frameLength + 4);
  }
  this._buf = buf;
  cb();
};

FrameDecoder.prototype._flush = function () {
  if (this._buf.length || this._bufs.length) {
    this.emit('error', new Error('trailing data'));
  }
};

/**
 * Standard framing stream.
 *
 * @param `frameSize` {Number} (Maximum) size in bytes of each frame. The last
 * frame might be shorter. Defaults to 4096.
 *
 */
function FrameEncoder() {
  stream.Transform.call(this, {writableObjectMode: true});
  this.on('finish', function () { this.push(null); });
}
util.inherits(FrameEncoder, stream.Transform);

FrameEncoder.prototype._transform = function (obj, encoding, cb) {
  var bufs = obj.payload;
  var i, l, buf;
  for (i = 0, l = bufs.length; i < l; i++) {
    buf = bufs[i];
    this.push(intBuffer(buf.length));
    this.push(buf);
  }
  this.push(intBuffer(0));
  cb();
};

/**
 * Netty-compatible decoding stream.
 *
 */
function NettyDecoder() {
  stream.Transform.call(this, {readableObjectMode: true});
  this._id = undefined;
  this._frameCount = 0;
  this._buf = new Buffer(0);
  this._bufs = [];

  this.on('finish', function () { this.push(null); });
}
util.inherits(NettyDecoder, stream.Transform);

NettyDecoder.prototype._transform = function (buf, encoding, cb) {
  buf = Buffer.concat([this._buf, buf]);

  while (true) {
    if (this._id === undefined) {
      if (buf.length < 8) {
        this._buf = buf;
        cb();
        return;
      }
      this._id = buf.readInt32BE(0);
      this._frameCount = buf.readInt32BE(4);
      buf = buf.slice(8);
    }

    var frameLength;
    while (
      this._frameCount &&
      buf.length >= 4 &&
      buf.length >= (frameLength = buf.readInt32BE(0)) + 4
    ) {
      this._frameCount--;
      this._bufs.push(buf.slice(4, frameLength + 4));
      buf = buf.slice(frameLength + 4);
    }

    if (this._frameCount) {
      this._buf = buf;
      cb();
      return;
    } else {
      var obj = {id: this._id, payload: this._bufs};
      this._bufs = [];
      this._id = undefined;
      this.push(obj);
    }
  }
};

NettyDecoder.prototype._flush = function () {
  if (this._buf.length || this._bufs.length) {
    this.emit('error', new Error('trailing data'));
  }
};

/**
 * Netty-compatible encoding stream.
 *
 */
function NettyEncoder() {
  stream.Transform.call(this, {writableObjectMode: true});
  this.on('finish', function () { this.push(null); });
}
util.inherits(NettyEncoder, stream.Transform);

NettyEncoder.prototype._transform = function (obj, encoding, cb) {
  var bufs = obj.payload;
  var l = bufs.length;
  var buf;
  // Header: [ ID, number of frames ]
  buf = new Buffer(8);
  buf.writeInt32BE(obj.id, 0);
  buf.writeInt32BE(l, 4);
  this.push(buf);
  // Frames, each: [ length, bytes ]
  var i;
  for (i = 0; i < l; i++) {
    buf = bufs[i];
    this.push(intBuffer(buf.length));
    this.push(buf);
  }
  cb();
};

/**
 * Returns a buffer containing an integer's big-endian representation.
 *
 * @param n {Number} Integer.
 *
 */
function intBuffer(n) {
  var buf = new Buffer(4);
  buf.writeInt32BE(n);
  return buf;
}

/**
 * Decode a type used as prefix inside a buffer.
 *
 * @param type {Type} The type of the prefix.
 * @param buf {Buffer} Encoded bytes.
 *
 * This function will return an object `{head, tail}` where head contains the
 * decoded value and tail the rest of the buffer. An error will be thrown if
 * the prefix cannot be decoded.
 *
 */
function readHead(type, buf) {
  var tap = new Tap(buf);
  var head = type._read(tap);
  if (!tap.isValid()) {
    throw new Error(f('truncated %s', type));
  }
  return {head: head, tail: tap.buf.slice(tap.pos)};
}

/**
 * Wrap something in an error.
 *
 * @param message {String} The new error's message.
 * @param cause {Error} The cause of the error. It is available as `cause`
 * field on the outer error.
 *
 * This is used to keep the argument of emitters' `'error'` event errors.
 *
 */
function wrapError(message, cause) {
  var err = new Error(f('%s: %s', message, cause.message));
  err.cause = cause;
  return err;
}

/**
 * Check whether something is an error.
 *
 * @param any {Object} Any object.
 *
 */
function isError(any) {
  // Also not ideal, but avoids brittle `instanceof` checks.
  return !!any && Object.prototype.toString.call(any) === '[object Error]';
}

/**
 * Encode an error and optional header into a valid Avro response.
 *
 * @param err {Error} Error to encode.
 * @param header {Object} Optional response header.
 *
 */
function encodeError(err, header) {
  return Buffer.concat([
    header || new Buffer([0]), // Recover the header if possible.
    new Buffer([1, 0]), // Error flag and first union index.
    STRING_TYPE.toBuffer(err.message)
  ]);
}

/**
 * Check whether something is a stream.
 *
 * @param any {Object} Any object.
 *
 */
function isStream(any) {
  // This is a hacky way of checking that the transport is a stream-like
  // object. We unfortunately can't use `instanceof Stream` checks since
  // some libraries (e.g. websocket-stream) return streams which don't
  // inherit from it.
  return !!any.pipe;
}


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  Message: Message,
  Protocol: Protocol,
  Registry: Registry,
  createProtocol: createProtocol,
  streams: {
    FrameDecoder: FrameDecoder,
    FrameEncoder: FrameEncoder,
    NettyDecoder: NettyDecoder,
    NettyEncoder: NettyEncoder
  }
};

}).call(this,require('_process'),require("buffer").Buffer)
},{"./types":34,"./utils":35,"_process":13,"buffer":4,"events":6,"stream":24,"util":28}],33:[function(require,module,exports){
/* jshint node: true */

'use strict';

/**
 * IDL to schema parsing logic.
 *
 */

var files = require('./files'),
    utils = require('./utils'),
    path = require('path'),
    util = require('util');


var f = util.format;

/**
 * Assemble an IDL file into a decoded schema.
 *
 */
function assemble(fpath, opts, cb) {
  if (!cb && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }

  opts = opts || {};
  if (!opts.importHook) {
    opts.importHook = files.createImportHook();
  }

  var attrs = {types: [], messages: {}}; // Final result.
  var importedTypes = []; // Imported types, kept separate for ordering.
  var imports = []; // List of paths inside this file to import.
  var tk; // Tokenizer.

  opts.importHook(fpath, 'idl', function (err, str) {
    if (err) {
      cb(err);
      return;
    }

    if (!str) {
      // Skipped import (likely already imported).
      cb(null, {});
      return;
    }

    try {
      tk = new Tokenizer(str);
      tk.next(); // Prime tokenizer.
      readProtocol();
    } catch (err) {
      err.path = fpath; // To help debug which file caused the error.
      cb(err);
      return;
    }

    assembleImports();
  });

  function assembleImports(err, importAttrs) {
    if (err) {
      cb(err);
      return;
    }

    if (importAttrs) {
      // Merge in any imported attributes, first the types (where we don't need
      // to check for duplicates since `parse` will take care of it), then the
      // messages (where we need to, as duplicates will overwrite each other).
      (importAttrs.types || []).forEach(function (typeAttrs) {
        // Ensure the imported protocol's namespace is inherited correctly (it
        // might be different from the current one). Thanks to the IDLs
        // limitation of not allowing inline definition of named types, we are
        // also guaranteed to be correct (we don't need to walk the type's
        // attributes for named type declarations, they are all top level).
        if (typeAttrs.namespace === undefined) {
          typeAttrs.namespace = importAttrs.namespace || '';
        }
        importedTypes.push(typeAttrs);
      });
      try {
        Object.keys(importAttrs.messages || {}).forEach(function (name) {
          if (attrs.messages[name]) {
            throw new Error(f('duplicate message: %s', name));
          }
          attrs.messages[name] = importAttrs.messages[name];
        });
      } catch (err) {
        cb(err);
        return;
      }
    }

    var info = imports.shift();
    if (!info) {
      // We are done with this file. We prepend all imported types to this
      // file's and we can return the final result. We also perform a JSON
      // serialization rountrip to remove non-numerical attributes from unions
      // and transform Javadocs into strings.
      attrs.types = importedTypes.concat(attrs.types);
      cb(null, JSON.parse(JSON.stringify(attrs)));
    } else if (info.kind === 'idl') {
      assemble(info.path, opts, assembleImports);
    } else {
      // We are importing a protocol or schema file.
      opts.importHook(info.path, info.kind, function (err, str) {
        if (err) {
          cb(err);
          return;
        }
        switch (info.kind) {
        case 'protocol':
        case 'schema':
          try {
            var obj = JSON.parse(str);
          } catch (err) {
            err.path = info.path;
            cb(err);
            return;
          }
          assembleImports(null, info.kind === 'schema' ? {types: [obj]} : obj);
          break;
        default:
          assembleImports(new Error(f('invalid import kind: %s', info.kind)));
        }
      });
    }
  }

  function readProtocol() {
    while (tk.get().val === 'import') {
      readImport();
    }
    while (tk.get().val === '@') {
      readAnnotation(attrs);
    }
    tk.addJavadoc(attrs);
    tk.get({val: 'protocol'});
    attrs.protocol = tk.next({id: 'name'}).val;
    tk.next({val: '{'});
    tk.next();
    while (tk.get().val !== '}') {
      if (tk.get().val === 'import') {
        readImport();
      } else {
        var typeAttrs = readType();
        if (typeAttrs.name) {
          // This was a named type declaration. Not very clean to rely on this,
          // but since the IDL spec doesn't consistently delimit type
          // declaration (e.g. fixed end with `;` but other bracketed types
          // don't) we aren't able to tell whether this is the start of a
          // message otherwise.
          attrs.types.push(typeAttrs);
        } else {
          var oneWay = false;
          if (typeAttrs === 'void' || typeAttrs.type === 'void') {
            if (opts.oneWayVoid) {
              oneWay = true;
            }
            if (typeAttrs === 'void') {
              typeAttrs = 'null';
            } else {
              typeAttrs.type = 'null';
            }
          }
          readMessage(attrs, typeAttrs, oneWay);
        }
      }
    }
    tk.next({id: '(eof)'});
  }

  function readImport() {
    tk.get({val: 'import'});
    var kind = tk.next({id: 'name'}).val;
    var fname = JSON.parse(tk.next({id: 'string'}).val);
    imports.push({kind: kind, path: path.join(path.dirname(fpath), fname)});
    tk.next({val: ';'});
    tk.next();
  }

  function readAnnotation(attrs) {
    tk.get({val: '@'});
    // Annotations are allowed to have names which aren't valid Avro names,
    // we must advance until we hit the first left parenthesis.
    var parts = [];
    while (tk.next().val !== '(') {
      parts.push(tk.get().val);
    }
    attrs[parts.join('')] = tk.next({id: 'json'}).val;
    tk.next({val: ')'});
    tk.next();
  }

  function readMessage(protocolAttrs, responseAttrs, oneWay) {
    var messageAttrs;
    if (opts.reassignJavadoc) {
      messageAttrs = {};
      messageAttrs.response = reassignJavadoc(responseAttrs, messageAttrs);
    } else {
      messageAttrs = {response: responseAttrs};
    }
    if (oneWay) {
      messageAttrs['one-way'] = true;
    }
    while (tk.get().val === '@') {
      readAnnotation(messageAttrs);
    }

    var name = tk.get({id: 'name'}).val;
    if (protocolAttrs.messages[name]) {
      // We have to do this check here otherwise the duplicate will be
      // overwritten (and `parse` won't be able to catch it).
      throw new Error(f('duplicate message: %s', name));
    }

    messageAttrs.request = [];
    tk.next({val: '('});
    if (tk.next().val !== ')') {
      tk.prev();
      do {
        tk.next(); // Skip `(` or `,`.
        messageAttrs.request.push(readField());
      } while (tk.get().val !== ')');
    }
    if (tk.next().val === 'throws') {
      // It doesn't seem like the IDL allows multiple error types, even though
      // the spec always prescribes a union (or they don't indicate which
      // syntax to use). To be safe, we'll only allow one custom error type.
      tk.next();
      messageAttrs.errors = [readType()];
    } else if (tk.get().val === 'oneway') {
      tk.next();
      messageAttrs['one-way'] = true;
    }
    tk.get({val: ';'});
    protocolAttrs.messages[name] = messageAttrs;
    tk.next();
  }

  function readField() {
    var attrs = {type: readType()};
    if (opts.reassignJavadoc) {
      attrs.type = reassignJavadoc(attrs.type, attrs);
    }
    while (tk.get().val === '@') {
      readAnnotation(attrs);
    }
    tk.addJavadoc(attrs);
    attrs.name = tk.get({id: 'name'}).val;
    if (tk.next().val === '=') {
      attrs['default'] = tk.next({id: 'json'}).val;
      tk.next();
    }
    return attrs;
  }

  function readType() {
    var attrs = {};
    while (tk.get().val === '@') {
      readAnnotation(attrs);
    }
    tk.addJavadoc(attrs);

    switch (tk.get().val) {
    case 'record':
    case 'error':
      return readRecord(attrs);
    case 'fixed':
      return readFixed(attrs);
    case 'enum':
      return readEnum(attrs);
    case 'map':
      return readMap(attrs);
    case 'array':
      return readArray(attrs);
    case 'union':
      return readUnion(attrs);
    default:
      var type = tk.get().val;
      tk.next();
      if (Object.keys(attrs).length) {
        attrs.type = type;
        return attrs;
      } else {
        return type;
      }
    }
  }

  function readFixed(attrs) {
    attrs.type = tk.get({val: 'fixed'}).val;
    attrs.name = tk.next({id: 'name'}).val;
    tk.next({val: '('});
    attrs.size = parseInt(tk.next({id: 'number'}).val);
    tk.next({val: ')'});
    if (tk.next().val === ';') {
      tk.next();
    }
    return attrs;
  }

  function readMap(attrs) {
    attrs.type = tk.get({val: 'map'}).val;
    tk.next({val: '<'});
    tk.next();
    attrs.values = readType();
    tk.get({val: '>'});
    tk.next();
    return attrs;
  }

  function readArray(attrs) {
    attrs.type = tk.get({val: 'array'}).val;
    tk.next({val: '<'});
    tk.next();
    attrs.items = readType();
    tk.get({val: '>'});
    tk.next();
    return attrs;
  }

  function readEnum(attrs) {
    attrs.type = tk.get({val: 'enum'}).val;
    attrs.name = tk.next({id: 'name'}).val;
    tk.next({val: '{'});
    attrs.symbols = [];
    do {
      attrs.symbols.push(tk.next().val);
    } while (tk.next().val !== '}');
    tk.next();
    return attrs;
  }

  function readUnion(attrs) {
    var arr = [];
    tk.get({val: 'union'});
    tk.next({val: '{'});
    do {
      tk.next();
      arr.push(readType());
    } while (tk.get().val !== '}');
    tk.next();
    Object.keys(attrs).forEach(function (name) {
      // We can do this since `JSON.stringify` will ignore non-numeric keys on
      // array objects. This lets us be consistent with field and message
      // attribute transfer (e.g. for `doc` and `order`).
      arr[name] = attrs[name];
    });
    return arr;
  }

  function readRecord(attrs) {
    attrs.type = tk.get({id: 'name'}).val;
    attrs.name = tk.next({id: 'name'}).val;
    attrs.fields = [];
    tk.next({val: '{'});
    while (tk.next().val !== '}') {
      attrs.fields.push(readField());
      tk.get({val: ';'});
    }
    tk.next();
    return attrs;
  }
}

// Helpers.

/**
 * Simple class to split an input string into tokens.
 *
 * There are different types of tokens, characterized by their `id`:
 *
 * + `number` numbers.
 * + `name` references.
 * + `string` double-quoted.
 * + `operator`, anything else, always single character.
 * + `json`, special, must be asked for (the tokenizer doesn't have enough
 *   context to predict these).
 *
 * This tokenizer also handles Javadoc extraction, via the `addJavadoc` method.
 *
 */
function Tokenizer(str) {
  this._str = str;
  this._pos = 0;
  this._queue = new BoundedQueue(3); // Bounded queue of last emitted tokens.
  this._token = undefined; // Current token.
  this._doc = undefined; // Javadoc.
}

Tokenizer.prototype.get = function (opts) {
  if (opts && opts.id && opts.id !== this._token.id) {
    throw this.error(f('expected %s but got %s', opts.id, this._token.val));
  } else if (opts && opts.val && opts.val !== this._token.val) {
    throw this.error(f('expected %s but got %s', opts.val, this._token.val));
  } else {
    return this._token;
  }
};

Tokenizer.prototype.next = function (opts) {
  this._skip();
  this._queue.push(this._pos);
  var pos = this._pos;
  var str = this._str;
  var c = str.charAt(pos);
  var id;

  if (!c) {
    if (opts && opts.id === '(eof)') {
      return {id: '(eof)'};
    } else {
      throw this.error('unexpected end of input');
    }
  }

  if (opts && opts.id === 'json') {
    id = 'json';
    this._pos = this._endOfJson();
  } else if (c === '"') {
    id = 'string';
    this._pos = this._endOfString();
  } else if (/[0-9]/.test(c)) {
    id = 'number';
    this._pos = this._endOf(/[0-9]/);
  } else if (/[`A-Za-z_.]/.test(c)) {
    id = 'name';
    this._pos = this._endOf(/[`A-Za-z0-9_.]/);
  } else {
    id = 'operator';
    this._pos = pos + 1;
  }

  this._token = {id: id, val: str.slice(pos, this._pos)};
  if (id === 'json') {
    // Let's be nice and give a more helpful error message when this occurs
    // (JSON parsing errors wouldn't let us find the location otherwise).
    try {
      this._token.val = JSON.parse(this._token.val);
    } catch (err) {
      throw this.error('invalid JSON');
    }
  } else if (id === 'name') {
    // Unescape names (our parser doesn't need them).
    this._token.val = this._token.val.replace(/`/g, '');
  }
  return this.get(opts);
};

Tokenizer.prototype.prev = function (opts) {
  var pos = this._queue.pop();
  if (pos === undefined) {
    throw new Error('cannot backtrack more');
  }
  this._pos = pos;
  return this.get(opts);
};

Tokenizer.prototype.error = function (msg) {
  var pos = this._queue.peek() || 1; // Use after whitespace position.
  var str = this._str;
  var lineNum = 1;
  var lineStart = 0;
  var i;
  for (i = 0; i < pos; i++) {
    if (str.charAt(i) === '\n') {
      lineNum++;
      lineStart = i;
    }
  }
  var err = new Error(msg);
  err.lineNum = lineNum;
  err.colNum = pos - lineStart;
  return err;
};

Tokenizer.prototype.addJavadoc = function (attrs) {
  if (this._doc === undefined || attrs.doc !== undefined) {
    return;
  }
  attrs.doc = this._doc;
  this._doc = undefined;
};

/**
 * Skip whitespace and comments.
 *
 */
Tokenizer.prototype._skip = function () {
  var str = this._str;
  var pos, c; // `pos` used for javadocs.

  while ((c = str.charAt(this._pos)) && /\s/.test(c)) {
    this._pos++;
  }
  if (c === '/') {
    switch (str.charAt(this._pos + 1)) {
    case '/':
      this._pos += 2;
      while ((c = str.charAt(this._pos)) && c !== '\n') {
        this._pos++;
      }
      return this._skip();
    case '*':
      this._pos += 2;
      if (str.charAt(this._pos) === '*') {
        pos = this._pos + 1;
      }
      while ((c = str.charAt(this._pos++))) {
        if (c === '*' && str.charAt(this._pos) === '/') {
          this._pos++;
          if (pos !== undefined) {
            this._doc = new Javadoc(str.slice(pos, this._pos - 2));
          }
          return this._skip();
        }
      }
      throw this.error('unterminated comment');
    }
  }
};

/**
 * Generic end of method.
 *
 */
Tokenizer.prototype._endOf = function (pat) {
  var pos = this._pos;
  var str = this._str;
  while (pat.test(str.charAt(pos))) {
    pos++;
  }
  return pos;
};

/**
 * Find end of a string.
 *
 * The specification doesn't explicitly say so, but IDLs likely only allow
 * double quotes for strings (C- and Java-style).
 *
 */
Tokenizer.prototype._endOfString = function () {
  var pos = this._pos + 1; // Skip first double quote.
  var str = this._str;
  var c;
  while ((c = str.charAt(pos))) {
    if (c === '"') {
      return pos + 1;
    }
    if (c === '\\') {
      pos += 2;
    } else {
      pos++;
    }
  }
  throw this.error('unterminated string');
};

/**
 * Returns end of JSON object, throwing an error if the end is reached first.
 *
 */
Tokenizer.prototype._endOfJson = function () {
  var pos = utils.jsonEnd(this._str, this._pos);
  if (pos < 0) {
    throw new Error('invalid JSON at ' + this._pos);
  }
  return pos;
};

/**
 * Simple bounded queue.
 *
 * Not the fastest, but will definitely do.
 *
 */
function BoundedQueue(length) {
  this._length = length | 0;
  this._data = [];
}

BoundedQueue.prototype.push = function (val) {
  this._data.push(val);
  if (this._data.length > this._length) {
    this._data.shift();
  }
};

BoundedQueue.prototype.peek = function () {
  return this._data[this._data.length - 1];
};

BoundedQueue.prototype.pop = function () { return this._data.pop(); };

/**
 * Javadoc wrapper class.
 *
 * This is used to be able to distinguish between normal `doc` annotations and
 * Javadoc comments, to correctly support the `reassignJavadoc` option.
 *
 * The parsing done is very simple and simply removes the line prefixes and
 * leading / trailing empty lines. It's better to be conservative with
 * formatting rather than risk losing information.
 *
 */
function Javadoc(str) {
  str = str.replace(/^[ \t]+|[ \t]+$/g, ''); // Trim whitespace.
  var lines = str.split('\n').map(function (line, i) {
    return i ? line.replace(/^\s*\*\s?/, '') : line;
  });
  while (!lines[0]) {
    lines.shift();
  }
  while (!lines[lines.length - 1]) {
    lines.pop();
  }
  this._str = lines.join('\n');
}

Javadoc.prototype.toJSON = function () { return this._str; };

/**
 * Transfer a key from an object to another and return the new source.
 *
 * If the source becomes an object with a single type attribute set, its `type`
 * attribute is returned instead.
 *
 */
function reassignJavadoc(from, to) {
  if (!(from.doc instanceof Javadoc)) {
    // Nothing to transfer.
    return from;
  }
  to.doc = from.doc;
  delete from.doc;
  return Object.keys(from).length === 1 ? from.type : from;
}


module.exports = {
  BoundedQueue: BoundedQueue,
  Tokenizer: Tokenizer,
  assemble: assemble
};

},{"./files":31,"./utils":35,"path":11,"util":28}],34:[function(require,module,exports){
(function (Buffer){
/* jshint node: true */

// TODO: Use `toFastProperties` on type reverse indices.
// TODO: Allow configuring when to write the size when writing arrays and maps,
// and customizing their block size.
// TODO: Code-generate `compare` and `clone` record and union methods.

'use strict';

/**
 * This module defines all Avro data types and their serialization logic.
 *
 */

var utils = require('./utils'),
    buffer = require('buffer'), // For `SlowBuffer`.
    util = require('util');


// Convenience imports.
var Tap = utils.Tap;
var f = util.format;

// All non-union concrete (i.e. non-logical) Avro types.
var TYPES = {
  'array': ArrayType,
  'boolean': BooleanType,
  'bytes': BytesType,
  'double': DoubleType,
  'enum': EnumType,
  'error': RecordType,
  'fixed': FixedType,
  'float': FloatType,
  'int': IntType,
  'long': LongType,
  'map': MapType,
  'null': NullType,
  'record': RecordType,
  'string': StringType
};

// Addition by Ajai
// Allow hyphens to be part of field name (including enums)
// Valid (field, type, and symbol) name regex.
var NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_-]*$/;

// Random generator.
var RANDOM = new utils.Lcg();

// Encoding tap (shared for performance).
var TAP = new Tap(new buffer.SlowBuffer(1024));

// Currently active logical type, used for name redirection.
var LOGICAL_TYPE = null;

// Variable used to decide whether to include logical attributes when getting a
// type's schema. A global variable is the simplest way to "pass an argument"
// to JSON stringify's replacer function.
var EXPORT_ATTRS = false;

/**
 * Schema parsing entry point.
 *
 * It isn't exposed directly but called from `parse` inside `index.js` (node)
 * or `avsc.js` (browserify) which each add convenience functionality.
 *
 */
function createType(attrs, opts) {
  if (attrs === null) {
    // Let's be helpful for this common error.
    throw new Error('invalid type: null (did you mean "null"?)');
  }
  if (Type.isType(attrs)) {
    return attrs;
  }

  opts = opts || {};
  opts.registry = opts.registry || {};

  var type;
  if (typeof attrs == 'string') { // Type reference.
    attrs = qualify(attrs, opts.namespace);
    type = opts.registry[attrs];
    if (type) {
      // Type was already defined, return it.
      return type;
    }
    if (isPrimitive(attrs)) {
      // Reference to a primitive type. These are also defined names by default
      // so we create the appropriate type and it to the registry for future
      // reference.
      return opts.registry[attrs] = createType({type: attrs}, opts);
    }
    throw new Error(f('undefined type name: %s', attrs));
  }

  if (opts.typeHook && (type = opts.typeHook(attrs, opts))) {
    if (!Type.isType(type)) {
      throw new Error(f('invalid typehook return value: %j', type));
    }
    return type;
  }

  if (attrs.logicalType && opts.logicalTypes && !LOGICAL_TYPE) {
    var DerivedType = opts.logicalTypes[attrs.logicalType];
    if (DerivedType) {
      var namespace = opts.namespace;
      var registry = {};
      Object.keys(opts.registry).forEach(function (key) {
        registry[key] = opts.registry[key];
      });
      try {
        return new DerivedType(attrs, opts);
      } catch (err) {
        if (opts.assertLogicalTypes) {
          // The spec mandates that we fall through to the underlying type if
          // the logical type is invalid. We provide this option to ease
          // debugging.
          throw err;
        }
        LOGICAL_TYPE = null;
        opts.namespace = namespace;
        opts.registry = registry;
      }
    }
  }

  if (Array.isArray(attrs)) { // Union.
    var UnionType = opts.wrapUnions ? WrappedUnionType : UnwrappedUnionType;
    type = new UnionType(attrs, opts);
  } else { // New type definition.
    type = (function (typeName) {
      var Type = TYPES[typeName];
      if (Type === undefined) {
        throw new Error(f('unknown type: %j', typeName));
      }
      return new Type(attrs, opts);
    })(attrs.type);
  }
  return type;
}

/**
 * "Abstract" base Avro type.
 *
 * This class' constructor will register any named types to support recursive
 * schemas. All type values are represented in memory similarly to their JSON
 * representation, except for:
 *
 * + `bytes` and `fixed` which are represented as `Buffer`s.
 * + `union`s which will be "unwrapped" unless the `wrapUnions` option is set.
 *
 *  See individual subclasses for details.
 *
 */
function Type(attrs, opts) {
  var type = LOGICAL_TYPE || this;
  LOGICAL_TYPE = null;

  // Lazily instantiated hash string. It will be generated the first time the
  // type's default fingerprint is computed (for example when using `equals`).
  this._hs = undefined;
  this._name = undefined;
  this._aliases = undefined;

  if (attrs) {
    // This is a complex (i.e. non-primitive) type.
    var name = attrs.name;
    var namespace = attrs.namespace === undefined ?
      opts && opts.namespace :
      attrs.namespace;
    if (name !== undefined) {
      // This isn't an anonymous type.
      name = qualify(name, namespace);
      if (isPrimitive(name)) {
        // Avro doesn't allow redefining primitive names.
        throw new Error(f('cannot rename primitive type: %j', name));
      }
      var registry = opts && opts.registry;
      if (registry) {
        if (registry[name] !== undefined) {
          throw new Error(f('duplicate type name: %s', name));
        }
        registry[name] = type;
      }
    } else if (opts && opts.noAnonymousTypes) {
      throw new Error(f('missing name property in schema: %j', attrs));
    }
    this._name = name;
    this._aliases = attrs.aliases ?
      attrs.aliases.map(function (s) { return qualify(s, namespace); }) :
      [];
  }
}

Type.isType = function (/* any, [prefix] ... */) {
  var l = arguments.length;
  if (!l) {
    return false;
  }

  var any = arguments[0];
  if (
    !any ||
    typeof any._update != 'function' ||
    typeof any.getTypeName != 'function'
  ) {
    // Not fool-proof, but most likely good enough.
    return false;
  }

  if (l === 1) {
    // No type names specified, we are done.
    return true;
  }

  // We check if at least one of the prefixes matches.
  var typeName = any.getTypeName();
  var i;
  for (i = 1; i < l; i++) {
    if (typeName.indexOf(arguments[i]) === 0) {
      return true;
    }
  }
  return false;
};

Type.__reset = function (size) { TAP.buf = new buffer.SlowBuffer(size); };

Type.prototype.createResolver = function (type, opts) {
  if (!Type.isType(type)) {
    // More explicit error message than the "incompatible type" thrown
    // otherwise (especially because of the overridden `toJSON` method).
    throw new Error(f('not a type: %j', type));
  }

  if (!Type.isType(this, 'logical') && Type.isType(type, 'logical')) {
    // Trying to read a logical type as a built-in: unwrap the logical type.
    return this.createResolver(type._underlyingType, opts);
  }

  opts = opts || {};
  opts.registry = opts.registry || {};

  var resolver, key;
  if (
    Type.isType(this, 'record', 'error') &&
    Type.isType(type, 'record', 'error')
  ) {
    // We allow conversions between records and errors.
    key = this._name + ':' + type._name; // ':' is illegal in Avro type names.
    resolver = opts.registry[key];
    if (resolver) {
      return resolver;
    }
  }

  resolver = new Resolver(this);
  if (key) { // Register resolver early for recursive schemas.
    opts.registry[key] = resolver;
  }

  if (Type.isType(type, 'union')) {
    var resolvers = type._types.map(function (t) {
      return this.createResolver(t, opts);
    }, this);
    resolver._read = function (tap) {
      var index = tap.readLong();
      var resolver = resolvers[index];
      if (resolver === undefined) {
        throw new Error(f('invalid union index: %s', index));
      }
      return resolvers[index]._read(tap);
    };
  } else {
    this._update(resolver, type, opts);
  }

  if (!resolver._read) {
    throw new Error(f('cannot read %s as %s', type, this));
  }
  return resolver;
};

Type.prototype.decode = function (buf, pos, resolver) {
  var tap = new Tap(buf, pos);
  var val = readValue(this, tap, resolver);
  if (!tap.isValid()) {
    return {value: undefined, offset: -1};
  }
  return {value: val, offset: tap.pos};
};

Type.prototype.encode = function (val, buf, pos) {
  var tap = new Tap(buf, pos);
  this._write(tap, val);
  if (!tap.isValid()) {
    // Don't throw as there is no way to predict this. We also return the
    // number of missing bytes to ease resizing.
    return buf.length - tap.pos;
  }
  return tap.pos;
};

Type.prototype.fromBuffer = function (buf, resolver, noCheck) {
  var tap = new Tap(buf);
  var val = readValue(this, tap, resolver, noCheck);
  if (!tap.isValid()) {
    throw new Error('truncated buffer');
  }
  if (!noCheck && tap.pos < buf.length) {
    throw new Error('trailing data');
  }
  return val;
};

Type.prototype.toBuffer = function (val) {
  TAP.pos = 0;
  this._write(TAP, val);
  if (!TAP.isValid()) {
    Type.__reset(2 * TAP.pos);
    TAP.pos = 0;
    this._write(TAP, val);
  }
  var buf = new Buffer(TAP.pos);
  TAP.buf.copy(buf, 0, 0, TAP.pos);
  return buf;
};

Type.prototype.fromString = function (str) {
  return this._copy(JSON.parse(str), {coerce: 2});
};

Type.prototype.toString = function (val) {
  if (val === undefined) {
    // Consistent behavior with standard `toString` expectations.
    return this.getSchema({noDeref: true});
  }
  return JSON.stringify(this._copy(val, {coerce: 3}));
};

Type.prototype.clone = function (val, opts) {
  if (opts) {
    opts = {
      coerce: !!opts.coerceBuffers | 0, // Coerce JSON to Buffer.
      fieldHook: opts.fieldHook,
      qualifyNames: !!opts.qualifyNames,
      wrap: !!opts.wrapUnions | 0 // Wrap first match into union.
    };
    return this._copy(val, opts);
  } else {
    // If no modifications are required, we can get by with a serialization
    // roundtrip (generally much faster than a standard deep copy).
    return this.fromBuffer(this.toBuffer(val));
  }
};

Type.prototype.isValid = function (val, opts) {
  // We only have a single flag for now, so no need to complicate things.
  var flags = (opts && opts.noUndeclaredFields) | 0;
  var errorHook = opts && opts.errorHook;
  var hook, path;
  if (errorHook) {
    path = [];
    hook = function (any, type) {
      errorHook.call(this, path.slice(), any, type, val);
    };
  }
  return this._check(val, flags, hook, path);
};

Type.prototype.compareBuffers = function (buf1, buf2) {
  return this._match(new Tap(buf1), new Tap(buf2));
};

Type.prototype.getName = function (asBranch) {
  var type = Type.isType(this, 'logical') ? this._underlyingType : this;
  if (type._name || !asBranch) {
    return type._name;
  }
  return Type.isType(this, 'union') ? undefined : type.getTypeName();
};

Type.prototype.getSchema = function (opts) { return stringify(this, opts); };

Type.prototype.equals = function (type) {
  return (
    Type.isType(type) &&
    this.getFingerprint().equals(type.getFingerprint())
  );
};

Type.prototype.getFingerprint = function (algorithm) {
  if (!algorithm) {
    if (!this._hs) {
      this._hs = utils.getHash(this.getSchema()).toString('binary');
    }
    return new Buffer(this._hs, 'binary');
  } else {
    return utils.getHash(this.getSchema(), algorithm);
  }
};

Type.prototype.inspect = function () {
  var typeName = this.getTypeName();
  var className = getClassName(typeName);
  if (isPrimitive(typeName)) {
    // The class name is sufficient to identify the type.
    return f('<%s>', className);
  } else {
    // We add a little metadata for convenience.
    var obj = JSON.parse(this.getSchema({exportAttrs: true, noDeref: true}));
    if (typeof obj == 'object' && !Type.isType(this, 'logical')) {
      obj.type = undefined; // Would be redundant with constructor name.
    }
    return f('<%s %j>', className, obj);
  }
};

Type.prototype._check = utils.abstractFunction;
Type.prototype._copy = utils.abstractFunction;
Type.prototype._match = utils.abstractFunction;
Type.prototype._read = utils.abstractFunction;
Type.prototype._skip = utils.abstractFunction;
Type.prototype._update = utils.abstractFunction;
Type.prototype._write = utils.abstractFunction;
Type.prototype.compare = utils.abstractFunction;
Type.prototype.getTypeName = utils.abstractFunction;
Type.prototype.random = utils.abstractFunction;

// Implementations.

/**
 * Base primitive Avro type.
 *
 * Most of the primitive types share the same cloning and resolution
 * mechanisms, provided by this class. This class also lets us conveniently
 * check whether a type is a primitive using `instanceof`.
 *
 */
function PrimitiveType() { Type.call(this); }
util.inherits(PrimitiveType, Type);

PrimitiveType.prototype._update = function (resolver, type) {
  if (type.constructor === this.constructor) {
    resolver._read = this._read;
  }
};

PrimitiveType.prototype._copy = function (val) {
  this._check(val, undefined, throwInvalidError);
  return val;
};

PrimitiveType.prototype.compare = utils.compare;

PrimitiveType.prototype.toJSON = function () { return this.getTypeName(); };

/**
 * Nulls.
 *
 */
function NullType() { PrimitiveType.call(this); }
util.inherits(NullType, PrimitiveType);

NullType.prototype._check = function (val, flags, hook) {
  var b = val === null;
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

NullType.prototype._read = function () { return null; };

NullType.prototype._skip = function () {};

NullType.prototype._write = function (tap, val) {
  if (val !== null) {
    throwInvalidError(val, this);
  }
};

NullType.prototype._match = function () { return 0; };

NullType.prototype.compare = NullType.prototype._match;

NullType.prototype.getTypeName = function () { return 'null'; };

NullType.prototype.random = NullType.prototype._read;

/**
 * Booleans.
 *
 */
function BooleanType() { PrimitiveType.call(this); }
util.inherits(BooleanType, PrimitiveType);

BooleanType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'boolean';
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

BooleanType.prototype._read = function (tap) { return tap.readBoolean(); };

BooleanType.prototype._skip = function (tap) { tap.skipBoolean(); };

BooleanType.prototype._write = function (tap, val) {
  if (typeof val != 'boolean') {
    throwInvalidError(val, this);
  }
  tap.writeBoolean(val);
};

BooleanType.prototype._match = function (tap1, tap2) {
  return tap1.matchBoolean(tap2);
};

BooleanType.prototype.getTypeName = function () { return 'boolean'; };

BooleanType.prototype.random = function () { return RANDOM.nextBoolean(); };

/**
 * Integers.
 *
 */
function IntType() { PrimitiveType.call(this); }
util.inherits(IntType, PrimitiveType);

IntType.prototype._check = function (val, flags, hook) {
  var b = val === (val | 0);
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

IntType.prototype._read = function (tap) { return tap.readInt(); };

IntType.prototype._skip = function (tap) { tap.skipInt(); };

IntType.prototype._write = function (tap, val) {
  if (val !== (val | 0)) {
    throwInvalidError(val, this);
  }
  tap.writeInt(val);
};

IntType.prototype._match = function (tap1, tap2) {
  return tap1.matchInt(tap2);
};

IntType.prototype.getTypeName = function () { return 'int'; };

IntType.prototype.random = function () { return RANDOM.nextInt(1000) | 0; };

/**
 * Longs.
 *
 * We can't capture all the range unfortunately since JavaScript represents all
 * numbers internally as `double`s, so the default implementation plays safe
 * and throws rather than potentially silently change the data. See `__with` or
 * `AbstractLongType` below for a way to implement a custom long type.
 *
 */
function LongType() { PrimitiveType.call(this); }
util.inherits(LongType, PrimitiveType);

LongType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'number' && val % 1 === 0 && isSafeLong(val);
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

LongType.prototype._read = function (tap) {
  var n = tap.readLong();
  if (!isSafeLong(n)) {
    throw new Error('potential precision loss');
  }
  return n;
};

LongType.prototype._skip = function (tap) { tap.skipLong(); };

LongType.prototype._write = function (tap, val) {
  if (typeof val != 'number' || val % 1 || !isSafeLong(val)) {
    throwInvalidError(val, this);
  }
  tap.writeLong(val);
};

LongType.prototype._match = function (tap1, tap2) {
  return tap1.matchLong(tap2);
};

LongType.prototype._update = function (resolver, type) {
  switch (type.getTypeName()) {
    case 'int':
    case 'long':
      resolver._read = type._read;
  }
};

LongType.prototype.getTypeName = function () { return 'long'; };

LongType.prototype.random = function () { return RANDOM.nextInt(); };

LongType.__with = function (methods, noUnpack) {
  methods = methods || {}; // Will give a more helpful error message.
  // We map some of the methods to a different name to be able to intercept
  // their input and output (otherwise we wouldn't be able to perform any
  // unpacking logic, and the type wouldn't work when nested).
  var mapping = {
    toBuffer: '_toBuffer',
    fromBuffer: '_fromBuffer',
    fromJSON: '_fromJSON',
    toJSON: '_toJSON',
    isValid: '_isValid',
    compare: 'compare'
  };
  var type = new AbstractLongType(noUnpack);
  Object.keys(mapping).forEach(function (name) {
    if (methods[name] === undefined) {
      throw new Error(f('missing method implementation: %s', name));
    }
    type[mapping[name]] = methods[name];
  });
  return type;
};

/**
 * Floats.
 *
 */
function FloatType() { PrimitiveType.call(this); }
util.inherits(FloatType, PrimitiveType);

FloatType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'number';
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

FloatType.prototype._read = function (tap) { return tap.readFloat(); };

FloatType.prototype._skip = function (tap) { tap.skipFloat(); };

FloatType.prototype._write = function (tap, val) {
  if (typeof val != 'number') {
    throwInvalidError(val, this);
  }
  tap.writeFloat(val);
};

FloatType.prototype._match = function (tap1, tap2) {
  return tap1.matchFloat(tap2);
};

FloatType.prototype._update = function (resolver, type) {
  switch (type.getTypeName()) {
    case 'float':
    case 'int':
    case 'long':
      resolver._read = type._read;
  }
};

FloatType.prototype.getTypeName = function () { return 'float'; };

FloatType.prototype.random = function () { return RANDOM.nextFloat(1e3); };

/**
 * Doubles.
 *
 */
function DoubleType() { PrimitiveType.call(this); }
util.inherits(DoubleType, PrimitiveType);

DoubleType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'number';
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

DoubleType.prototype._read = function (tap) { return tap.readDouble(); };

DoubleType.prototype._skip = function (tap) { tap.skipDouble(); };

DoubleType.prototype._write = function (tap, val) {
  if (typeof val != 'number') {
    throwInvalidError(val, this);
  }
  tap.writeDouble(val);
};

DoubleType.prototype._match = function (tap1, tap2) {
  return tap1.matchDouble(tap2);
};

DoubleType.prototype._update = function (resolver, type) {
  switch (type.getTypeName()) {
    case 'double':
    case 'float':
    case 'int':
    case 'long':
      resolver._read = type._read;
  }
};

DoubleType.prototype.getTypeName = function () { return 'double'; };

DoubleType.prototype.random = function () { return RANDOM.nextFloat(); };

/**
 * Strings.
 *
 */
function StringType() { PrimitiveType.call(this); }
util.inherits(StringType, PrimitiveType);

StringType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'string';
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

StringType.prototype._read = function (tap) { return tap.readString(); };

StringType.prototype._skip = function (tap) { tap.skipString(); };

StringType.prototype._write = function (tap, val) {
  if (typeof val != 'string') {
    throwInvalidError(val, this);
  }
  tap.writeString(val);
};

StringType.prototype._match = function (tap1, tap2) {
  return tap1.matchString(tap2);
};

StringType.prototype._update = function (resolver, type) {
  switch (type.getTypeName()) {
    case 'bytes':
    case 'string':
      resolver._read = this._read;
  }
};

StringType.prototype.getTypeName = function () { return 'string'; };

StringType.prototype.random = function () {
  return RANDOM.nextString(RANDOM.nextInt(32));
};

/**
 * Bytes.
 *
 * These are represented in memory as `Buffer`s rather than binary-encoded
 * strings. This is more efficient (when decoding/encoding from bytes, the
 * common use-case), idiomatic, and convenient.
 *
 * Note the coercion in `_copy`.
 *
 */
function BytesType() { PrimitiveType.call(this); }
util.inherits(BytesType, PrimitiveType);

BytesType.prototype._check = function (val, flags, hook) {
  var b = Buffer.isBuffer(val);
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

BytesType.prototype._read = function (tap) { return tap.readBytes(); };

BytesType.prototype._skip = function (tap) { tap.skipBytes(); };

BytesType.prototype._write = function (tap, val) {
  if (!Buffer.isBuffer(val)) {
    throwInvalidError(val, this);
  }
  tap.writeBytes(val);
};

BytesType.prototype._match = function (tap1, tap2) {
  return tap1.matchBytes(tap2);
};

BytesType.prototype._update = StringType.prototype._update;

BytesType.prototype._copy = function (obj, opts) {
  var buf;
  switch ((opts && opts.coerce) | 0) {
    case 3: // Coerce buffers to strings.
      this._check(obj, undefined, throwInvalidError);
      return obj.toString('binary');
    case 2: // Coerce strings to buffers.
      if (typeof obj != 'string') {
        throw new Error(f('cannot coerce to buffer: %j', obj));
      }
      buf = new Buffer(obj, 'binary');
      this._check(buf, undefined, throwInvalidError);
      return buf;
    case 1: // Coerce buffer JSON representation to buffers.
      if (!isJsonBuffer(obj)) {
        throw new Error(f('cannot coerce to buffer: %j', obj));
      }
      buf = new Buffer(obj.data);
      this._check(buf, undefined, throwInvalidError);
      return buf;
    default: // Copy buffer.
      this._check(obj, undefined, throwInvalidError);
      return new Buffer(obj);
  }
};

BytesType.prototype.compare = Buffer.compare;

BytesType.prototype.getTypeName = function () { return 'bytes'; };

BytesType.prototype.random = function () {
  return RANDOM.nextBuffer(RANDOM.nextInt(32));
};

/**
 * Base "abstract" Avro union type.
 *
 */
function UnionType(attrs, opts) {
  Type.call(this);

  if (!Array.isArray(attrs)) {
    throw new Error(f('non-array union schema: %j', attrs));
  }
  if (!attrs.length) {
    throw new Error('empty union');
  }
  this._types = attrs.map(function (obj) { return createType(obj, opts); });

  this._branchIndices = {};
  this._types.forEach(function (type, i) {
    if (Type.isType(type, 'union')) {
      throw new Error('unions cannot be directly nested');
    }
    var branch = type.getName(true);
    if (this._branchIndices[branch] !== undefined) {
      throw new Error(f('duplicate union branch name: %j', branch));
    }
    this._branchIndices[branch] = i;
  }, this);
}
util.inherits(UnionType, Type);

UnionType.prototype._skip = function (tap) {
  this._types[tap.readLong()]._skip(tap);
};

UnionType.prototype._match = function (tap1, tap2) {
  var n1 = tap1.readLong();
  var n2 = tap2.readLong();
  if (n1 === n2) {
    return this._types[n1]._match(tap1, tap2);
  } else {
    return n1 < n2 ? -1 : 1;
  }
};

UnionType.prototype.getTypes = function () { return this._types.slice(); };

UnionType.prototype.toJSON = function () { return this._types; };

/**
 * "Natural" union type.
 *
 * This representation doesn't require a wrapping object and is therefore
 * simpler and generally closer to what users expect. However it cannot be used
 * to represent all Avro unions since some lead to ambiguities (e.g. if two
 * number types are in the union).
 *
 * Currently, this union supports at most one type in each of the categories
 * below:
 *
 * + `null`
 * + `boolean`
 * + `int`, `long`, `float`, `double`
 * + `string`, `enum`
 * + `bytes`, `fixed`
 * + `array`
 * + `map`, `record`
 *
 */
function UnwrappedUnionType(attrs, opts) {
  UnionType.call(this, attrs, opts);

  this._logicalBranches = null;
  this._bucketIndices = {};
  this._types.forEach(function (type, index) {
    if (Type.isType(type, 'logical')) {
      if (!this._logicalBranches) {
        this._logicalBranches = [];
      }
      this._logicalBranches.push({index: index, type: type});
    } else {
      var bucket = getTypeBucket(type);
      if (this._bucketIndices[bucket] !== undefined) {
        throw new Error(f('ambiguous unwrapped union: %j', this));
      }
      this._bucketIndices[bucket] = index;
    }
  }, this);
}
util.inherits(UnwrappedUnionType, UnionType);

UnwrappedUnionType.prototype._getIndex = function (val) {
  var index = this._bucketIndices[getValueBucket(val)];
  if (this._logicalBranches) {
    // Slower path, we must run the value through all logical types.
    index = this._getLogicalIndex(val, index);
  }
  return index;
};

UnwrappedUnionType.prototype._getLogicalIndex = function (any, index) {
  var logicalBranches = this._logicalBranches;
  var i, l, branch;
  for (i = 0, l = logicalBranches.length; i < l; i++) {
    branch = logicalBranches[i];
    if (branch.type._check(any)) {
      if (index === undefined) {
        index = branch.index;
      } else {
        // More than one branch matches the value so we aren't guaranteed to
        // infer the correct type. We throw rather than corrupt data. This can
        // be fixed by "tightening" the logical types.
        throw new Error('ambiguous conversion');
      }
    }
  }
  return index;
};

UnwrappedUnionType.prototype._check = function (val, flags, hook, path) {
  var index = this._getIndex(val);
  var b = index !== undefined;
  if (b) {
    return this._types[index]._check(val, flags, hook, path);
  }
  if (hook) {
    hook(val, this);
  }
  return b;
};

UnwrappedUnionType.prototype._read = function (tap) {
  var index = tap.readLong();
  var branchType = this._types[index];
  if (branchType) {
    return branchType._read(tap);
  } else {
    throw new Error(f('invalid union index: %s', index));
  }
};

UnwrappedUnionType.prototype._write = function (tap, val) {
  var index = this._getIndex(val);
  if (index === undefined) {
    throwInvalidError(val, this);
  }
  tap.writeLong(index);
  if (val !== null) {
    this._types[index]._write(tap, val);
  }
};

UnwrappedUnionType.prototype._update = function (resolver, type, opts) {
  // jshint -W083
  // (The loop exits after the first function is created.)
  var i, l, typeResolver;
  for (i = 0, l = this._types.length; i < l; i++) {
    try {
      typeResolver = this._types[i].createResolver(type, opts);
    } catch (err) {
      continue;
    }
    resolver._read = function (tap) { return typeResolver._read(tap); };
    return;
  }
};

UnwrappedUnionType.prototype._copy = function (val, opts) {
  var coerce = opts && opts.coerce | 0;
  var wrap = opts && opts.wrap | 0;
  var index;
  if (wrap === 2) {
    // We are parsing a default, so always use the first branch's type.
    index = 0;
  } else {
    switch (coerce) {
      case 1:
        // Using the `coerceBuffers` option can cause corruption and erroneous
        // failures with unwrapped unions (in rare cases when the union also
        // contains a record which matches a buffer's JSON representation).
        if (isJsonBuffer(val) && this._bucketIndices.buffer !== undefined) {
          index = this._bucketIndices.buffer;
        } else {
          index = this._getIndex(val);
        }
        break;
      case 2:
        // Decoding from JSON, we must unwrap the value.
        if (val === null) {
          index = this._bucketIndices['null'];
        } else if (typeof val === 'object') {
          var keys = Object.keys(val);
          if (keys.length === 1) {
            index = this._branchIndices[keys[0]];
            val = val[keys[0]];
          }
        }
        break;
      default:
        index = this._getIndex(val);
    }
    if (index === undefined) {
      throwInvalidError(val, this);
    }
  }
  var type = this._types[index];
  if (val === null || wrap === 3) {
    return type._copy(val, opts);
  } else {
    switch (coerce) {
      case 3:
        // Encoding to JSON, we wrap the value.
        var obj = {};
        obj[type.getName(true)] = type._copy(val, opts);
        return obj;
      default:
        return type._copy(val, opts);
    }
  }
};

UnwrappedUnionType.prototype.compare = function (val1, val2) {
  var index1 = this._getIndex(val1);
  var index2 = this._getIndex(val2);
  if (index1 === undefined) {
    throwInvalidError(val1, this);
  } else if (index2 === undefined) {
    throwInvalidError(val2, this);
  } else if (index1 === index2) {
    return this._types[index1].compare(val1, val2);
  } else {
    return utils.compare(index1, index2);
  }
};

UnwrappedUnionType.prototype.getTypeName = function () {
  return 'union:unwrapped';
};

UnwrappedUnionType.prototype.random = function () {
  var index = RANDOM.nextInt(this._types.length);
  return this._types[index].random();
};

/**
 * Compatible union type.
 *
 * Values of this type are represented in memory similarly to their JSON
 * representation (i.e. inside an object with single key the name of the
 * contained type).
 *
 * This is not ideal, but is the most efficient way to unambiguously support
 * all unions. Here are a few reasons why the wrapping object is necessary:
 *
 * + Unions with multiple number types would have undefined behavior, unless
 *   numbers are wrapped (either everywhere, leading to large performance and
 *   convenience costs; or only when necessary inside unions, making it hard to
 *   understand when numbers are wrapped or not).
 * + Fixed types would have to be wrapped to be distinguished from bytes.
 * + Using record's constructor names would work (after a slight change to use
 *   the fully qualified name), but would mean that generic objects could no
 *   longer be valid records (making it inconvenient to do simple things like
 *   creating new records).
 *
 */
function WrappedUnionType(attrs, opts) {
  UnionType.call(this, attrs, opts);

  this._constructors = this._types.map(function (type) {
    // jshint -W054
    var name = type.getName(true);
    if (name === 'null') {
      return null;
    }
    var body;
    if (~name.indexOf('.')) { // Qualified name.
      body = 'this[\'' + name + '\'] = val;';
    } else {
      body = 'this.' + name + ' = val;';
    }
    var constructor = new Function('val', body);
    constructor.getBranchType = function () { return type; };
    return constructor;
  });
}
util.inherits(WrappedUnionType, UnionType);

WrappedUnionType.prototype._check = function (val, flags, hook, path) {
  var b = false;
  if (val === null) {
    // Shortcut type lookup in this case.
    b = this._branchIndices['null'] !== undefined;
  } else if (typeof val == 'object') {
    var keys = Object.keys(val);
    if (keys.length === 1) {
      // We require a single key here to ensure that writes are correct and
      // efficient as soon as a record passes this check.
      var name = keys[0];
      var index = this._branchIndices[name];
      if (index !== undefined) {
        if (hook) {
          // Slow path.
          path.push(name);
          b = this._types[index]._check(val[name], flags, hook, path);
          path.pop();
          return b;
        } else {
          return this._types[index]._check(val[name], flags);
        }
      }
    }
  }
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

WrappedUnionType.prototype._read = function (tap) {
  var index = tap.readLong();
  var Class = this._constructors[index];
  if (Class) {
    return new Class(this._types[index]._read(tap));
  } else if (Class === null) {
    return null;
  } else {
    throw new Error(f('invalid union index: %s', index));
  }
};

WrappedUnionType.prototype._write = function (tap, val) {
  var index, keys, name;
  if (val === null) {
    index = this._branchIndices['null'];
    if (index === undefined) {
      throwInvalidError(val, this);
    }
    tap.writeLong(index);
  } else {
    keys = Object.keys(val);
    if (keys.length === 1) {
      name = keys[0];
      index = this._branchIndices[name];
    }
    if (index === undefined) {
      throwInvalidError(val, this);
    }
    tap.writeLong(index);
    this._types[index]._write(tap, val[name]);
  }
};

WrappedUnionType.prototype._update = function (resolver, type, opts) {
  // jshint -W083
  // (The loop exits after the first function is created.)
  var i, l, typeResolver, Class;
  for (i = 0, l = this._types.length; i < l; i++) {
    try {
      typeResolver = this._types[i].createResolver(type, opts);
    } catch (err) {
      continue;
    }
    Class = this._constructors[i];
    if (Class) {
      resolver._read = function (tap) {
        return new Class(typeResolver._read(tap));
      };
    } else {
      resolver._read = function () { return null; };
    }
    return;
  }
};

WrappedUnionType.prototype._copy = function (val, opts) {
  var wrap = opts && opts.wrap | 0;
  if (wrap === 2) {
    // Promote into first type (used for schema defaults).
    if (val === null && this._constructors[0] === null) {
      return null;
    }
    return new this._constructors[0](this._types[0]._copy(val, opts));
  }
  if (val === null && this._branchIndices['null'] !== undefined) {
    return null;
  }

  var i, l, obj;
  if (typeof val == 'object') {
    var keys = Object.keys(val);
    if (keys.length === 1) {
      var name = keys[0];
      i = this._branchIndices[name];
      if (i === undefined && opts.qualifyNames) {
        // We are a bit more flexible than in `_check` here since we have
        // to deal with other serializers being less strict, so we fall
        // back to looking up unqualified names.
        var j, type;
        for (j = 0, l = this._types.length; j < l; j++) {
          type = this._types[j];
          if (type._name && name === unqualify(type._name)) {
            i = j;
            break;
          }
        }
      }
      if (i !== undefined) {
        obj = this._types[i]._copy(val[name], opts);
      }
    }
  }
  if (wrap === 1 && obj === undefined) {
    // Try promoting into first match (convenience, slow).
    i = 0;
    l = this._types.length;
    while (i < l && obj === undefined) {
      try {
        obj = this._types[i]._copy(val, opts);
      } catch (err) {
        i++;
      }
    }
  }
  if (obj !== undefined) {
    return wrap === 3 ? obj : new this._constructors[i](obj);
  }
  throwInvalidError(val, this);
};

WrappedUnionType.prototype.compare = function (val1, val2) {
  var name1 = val1 === null ? 'null' : Object.keys(val1)[0];
  var name2 = val2 === null ? 'null' : Object.keys(val2)[0];
  var index = this._branchIndices[name1];
  if (name1 === name2) {
    return name1 === 'null' ?
      0 :
      this._types[index].compare(val1[name1], val2[name1]);
  } else {
    return utils.compare(index, this._branchIndices[name2]);
  }
};

WrappedUnionType.prototype.getTypeName = function () {
  return 'union:wrapped';
};

WrappedUnionType.prototype.random = function () {
  var index = RANDOM.nextInt(this._types.length);
  var Class = this._constructors[index];
  if (!Class) {
    return null;
  }
  return new Class(this._types[index].random());
};

/**
 * Avro enum type.
 *
 * Represented as strings (with allowed values from the set of symbols). Using
 * integers would be a reasonable option, but the performance boost is arguably
 * offset by the legibility cost and the extra deviation from the JSON encoding
 * convention.
 *
 * An integer representation can still be used (e.g. for compatibility with
 * TypeScript `enum`s) by overriding the `EnumType` with a `LongType` (e.g. via
 * `parse`'s registry).
 *
 */
function EnumType(attrs, opts) {
  Type.call(this, attrs, opts);

  if (!Array.isArray(attrs.symbols) || !attrs.symbols.length) {
    throw new Error(f('invalid enum symbols: %j', attrs.symbols));
  }
  this._symbols = attrs.symbols;

  this._indices = {};
  this._symbols.forEach(function (symbol, i) {
    if (!isValidName(symbol)) {
      throw new Error(f('invalid %s symbol: %j', this, symbol));
    }
    if (this._indices[symbol] !== undefined) {
      throw new Error(f('duplicate %s symbol: %j', this, symbol));
    }
    this._indices[symbol] = i;
  }, this);
}
util.inherits(EnumType, Type);

EnumType.prototype._check = function (val, flags, hook) {
  var b = this._indices[val] !== undefined;
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

EnumType.prototype._read = function (tap) {
  var index = tap.readLong();
  var symbol = this._symbols[index];
  if (symbol === undefined) {
    throw new Error(f('invalid %s enum index: %s', this._name, index));
  }
  return symbol;
};

EnumType.prototype._skip = function (tap) { tap.skipLong(); };

EnumType.prototype._write = function (tap, val) {
  var index = this._indices[val];
  if (index === undefined) {
    throwInvalidError(val, this);
  }
  tap.writeLong(index);
};

EnumType.prototype._match = function (tap1, tap2) {
  return tap1.matchLong(tap2);
};

EnumType.prototype.compare = function (val1, val2) {
  return utils.compare(this._indices[val1], this._indices[val2]);
};

EnumType.prototype._update = function (resolver, type) {
  var symbols = this._symbols;
  if (
    type.getTypeName() === 'enum' &&
    (!type._name || ~getAliases(this).indexOf(type._name)) &&
    type._symbols.every(function (s) { return ~symbols.indexOf(s); })
  ) {
    resolver._symbols = type._symbols;
    resolver._read = type._read;
  }
};

EnumType.prototype._copy = function (val) {
  this._check(val, undefined, throwInvalidError);
  return val;
};

EnumType.prototype.getAliases = function () { return this._aliases; };

EnumType.prototype.getSymbols = function () { return this._symbols.slice(); };

EnumType.prototype.getTypeName = function () { return 'enum'; };

EnumType.prototype.random = function () {
  return RANDOM.choice(this._symbols);
};

EnumType.prototype.toJSON = function () {
  return {
    name: this._name,
    type: this.getTypeName(),
    symbols: this._symbols,
    aliases: this._aliases
  };
};

/**
 * Avro fixed type.
 *
 * Represented simply as a `Buffer`.
 *
 */
function FixedType(attrs, opts) {
  Type.call(this, attrs, opts);

  if (attrs.size !== (attrs.size | 0) || attrs.size < 1) {
    throw new Error(f('invalid %s fixed size', this.getName(true)));
  }
  this._size = attrs.size | 0;
}
util.inherits(FixedType, Type);

FixedType.prototype._check = function (val, flags, hook) {
  var b = Buffer.isBuffer(val) && val.length === this._size;
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

FixedType.prototype._read = function (tap) {
  return tap.readFixed(this._size);
};

FixedType.prototype._skip = function (tap) {
  tap.skipFixed(this._size);
};

FixedType.prototype._write = function (tap, val) {
  if (!Buffer.isBuffer(val) || val.length !== this._size) {
    throwInvalidError(val, this);
  }
  tap.writeFixed(val, this._size);
};

FixedType.prototype._match = function (tap1, tap2) {
  return tap1.matchFixed(tap2, this._size);
};

FixedType.prototype.compare = Buffer.compare;

FixedType.prototype._update = function (resolver, type) {
  if (
    type.getTypeName() === 'fixed' &&
    this._size === type._size &&
    (!type._name || ~getAliases(this).indexOf(type._name))
  ) {
    resolver._size = this._size;
    resolver._read = this._read;
  }
};

FixedType.prototype._copy = BytesType.prototype._copy;

FixedType.prototype.getAliases = function () { return this._aliases; };

FixedType.prototype.getSize = function () { return this._size; };

FixedType.prototype.getTypeName = function () { return 'fixed'; };

FixedType.prototype.random = function () {
  return RANDOM.nextBuffer(this._size);
};

FixedType.prototype.toJSON = function () {
  return {
    name: this._name,
    type: this.getTypeName(),
    size: this._size,
    aliases: this._aliases
  };
};

/**
 * Avro map.
 *
 * Represented as vanilla objects.
 *
 */
function MapType(attrs, opts) {
  Type.call(this);

  if (!attrs.values) {
    throw new Error(f('missing map values: %j', attrs));
  }
  this._values = createType(attrs.values, opts);

  // Addition by Edwin Elia
  var keys = attrs.keys;
  if (!keys) {
    keys = 'string';
  }
  this._keys = createType(keys, opts);
}
util.inherits(MapType, Type);

MapType.prototype._check = function (val, flags, hook, path) {
  if (!val || typeof val != 'object' || Array.isArray(val)) {
    if (hook) {
      hook(val, this);
    }
    return false;
  }

  var keys = Object.keys(val);
  var b = true;
  var i, l, j, key;
  if (hook) {
    // Slow path.
    j = path.length;
    path.push('');
    for (i = 0, l = keys.length; i < l; i++) {
      key = path[j] = keys[i];
      if (!this._values._check(val[key], flags, hook, path)) {
        b = false;
      }
    }
    path.pop();
  } else {
    for (i = 0, l = keys.length; i < l; i++) {
      if (!this._values._check(val[keys[i]], flags)) {
        return false;
      }
    }
  }
  return b;
};

MapType.prototype._read = function (tap) {
  var values = this._values;
  var val = {};
  var n;
  while ((n = readArraySize(tap))) {
    while (n--) {
      var key = tap.readString();
      val[key] = values._read(tap);
    }
  }
  return val;
};

MapType.prototype._skip = function (tap) {
  var values = this._values;
  var len, n;
  while ((n = tap.readLong())) {
    if (n < 0) {
      len = tap.readLong();
      tap.pos += len;
    } else {
      while (n--) {
        tap.skipString();
        values._skip(tap);
      }
    }
  }
};

MapType.prototype._write = function (tap, val) {
  if (!val || typeof val != 'object' || Array.isArray(val)) {
    throwInvalidError(val, this);
  }

  var values = this._values;
  var keys = Object.keys(val);
  var n = keys.length;
  var i, key;
  if (n) {
    tap.writeLong(n);
    for (i = 0; i < n; i++) {
      key = keys[i];
      tap.writeString(key);
      values._write(tap, val[key]);
    }
  }
  tap.writeLong(0);
};

MapType.prototype._match = function () {
  throw new Error('maps cannot be compared');
};

MapType.prototype._update = function (resolver, type, opts) {
  if (type.getTypeName() === 'map') {
    resolver._values = this._values.createResolver(type._values, opts);
    resolver._read = this._read;
  }
};

MapType.prototype._copy = function (val, opts) {
  if (val && typeof val == 'object' && !Array.isArray(val)) {
    var values = this._values;
    var keys = Object.keys(val);
    var i, l, key;
    var copy = {};
    for (i = 0, l = keys.length; i < l; i++) {
      key = keys[i];
      copy[key] = values._copy(val[key], opts);
    }
    return copy;
  }
  throwInvalidError(val, this);
};

MapType.prototype.compare = MapType.prototype._match;

MapType.prototype.getTypeName = function () { return 'map'; };

MapType.prototype.getValuesType = function () { return this._values; };

// Addition by Edwin Elia
MapType.prototype.getKeysType = function () { return this._keys; };

MapType.prototype.random = function () {
  var val = {};
  var i, l;
  for (i = 0, l = RANDOM.nextInt(10); i < l; i++) {
    val[RANDOM.nextString(RANDOM.nextInt(20))] = this._values.random();
  }
  return val;
};

MapType.prototype.toJSON = function () {
  return {type: this.getTypeName(), values: this._values};
};

/**
 * Avro array.
 *
 * Represented as vanilla arrays.
 *
 */
function ArrayType(attrs, opts) {
  Type.call(this);

  if (!attrs.items) {
    throw new Error(f('missing array items: %j', attrs));
  }
  this._items = createType(attrs.items, opts);
}
util.inherits(ArrayType, Type);

ArrayType.prototype._check = function (val, flags, hook, path) {
  if (!Array.isArray(val)) {
    if (hook) {
      hook(val, this);
    }
    return false;
  }

  var b = true;
  var i, l, j;
  if (hook) {
    // Slow path.
    j = path.length;
    path.push('');
    for (i = 0, l = val.length; i < l; i++) {
      path[j] = '' + i;
      if (!this._items._check(val[i], flags, hook, path)) {
        b = false;
      }
    }
    path.pop();
  } else {
    for (i = 0, l = val.length; i < l; i++) {
      if (!this._items._check(val[i], flags)) {
        return false;
      }
    }
  }
  return b;
};

ArrayType.prototype._read = function (tap) {
  var items = this._items;
  var val = [];
  var i, n;
  while ((n = tap.readLong())) {
    if (n < 0) {
      n = -n;
      tap.skipLong(); // Skip size.
    }
    for (i = 0; i < n; i++) {
      val[i] = items._read(tap);
    }
  }
  return val;
};

ArrayType.prototype._skip = function (tap) {
  var len, n;
  while ((n = tap.readLong())) {
    if (n < 0) {
      len = tap.readLong();
      tap.pos += len;
    } else {
      while (n--) {
        this._items._skip(tap);
      }
    }
  }
};

ArrayType.prototype._write = function (tap, val) {
  if (!Array.isArray(val)) {
    throwInvalidError(val, this);
  }

  var n = val.length;
  var i;
  if (n) {
    tap.writeLong(n);
    for (i = 0; i < n; i++) {
      this._items._write(tap, val[i]);
    }
  }
  tap.writeLong(0);
};

ArrayType.prototype._match = function (tap1, tap2) {
  var n1 = tap1.readLong();
  var n2 = tap2.readLong();
  var f;
  while (n1 && n2) {
    f = this._items._match(tap1, tap2);
    if (f) {
      return f;
    }
    if (!--n1) {
      n1 = readArraySize(tap1);
    }
    if (!--n2) {
      n2 = readArraySize(tap2);
    }
  }
  return utils.compare(n1, n2);
};

ArrayType.prototype._update = function (resolver, type, opts) {
  if (type.getTypeName() === 'array') {
    resolver._items = this._items.createResolver(type._items, opts);
    resolver._read = this._read;
  }
};

ArrayType.prototype._copy = function (val, opts) {
  if (!Array.isArray(val)) {
    throwInvalidError(val, this);
  }
  var items = new Array(val.length);
  var i, l;
  for (i = 0, l = val.length; i < l; i++) {
    items[i] = this._items._copy(val[i], opts);
  }
  return items;
};

ArrayType.prototype.compare = function (val1, val2) {
  var n1 = val1.length;
  var n2 = val2.length;
  var i, l, f;
  for (i = 0, l = Math.min(n1, n2); i < l; i++) {
    if ((f = this._items.compare(val1[i], val2[i]))) {
      return f;
    }
  }
  return utils.compare(n1, n2);
};

ArrayType.prototype.getItemsType = function () { return this._items; };

ArrayType.prototype.getTypeName = function () { return 'array'; };

ArrayType.prototype.random = function () {
  var arr = [];
  var i, l;
  for (i = 0, l = RANDOM.nextInt(10); i < l; i++) {
    arr.push(this._items.random());
  }
  return arr;
};

ArrayType.prototype.toJSON = function () {
  return {type: this.getTypeName(), items: this._items};
};

/**
 * Avro record.
 *
 * Values are represented as instances of a programmatically generated
 * constructor (similar to a "specific record"), available via the
 * `getRecordConstructor` method. This "specific record class" gives
 * significant speedups over using generics objects.
 *
 * Note that vanilla objects are still accepted as valid as long as their
 * fields match (this makes it much more convenient to do simple things like
 * update nested records).
 *
 * This type is also used for errors (similar, except for the extra `Error`
 * constructor call) and for messages (see comment below).
 *
 */
function RecordType(attrs, opts) {
  Type.call(this, attrs, opts);

  // Force creation of the options object in case we need to register this
  // record's name.
  opts = opts || {};

  if (!Array.isArray(attrs.fields)) {
    throw new Error(f('non-array record fields: %j', attrs.fields));
  }
  if (utils.hasDuplicates(attrs.fields, function (f) { return f.name; })) {
    throw new Error(f('duplicate field name: %j', attrs.fields));
  }

  // Save the namespace to restore it as we leave this record's scope.
  var namespace = opts.namespace;
  if (attrs.namespace !== undefined) {
    opts.namespace = attrs.namespace;
  } else {
    // Fully qualified names' namespaces are used when no explicit namespace
    // attribute was specified.
    var match = /^(.*)\.[^.]+$/.exec(this._name);
    opts.namespace = match ? match[1] : '';
  }
  this._fieldsMap = {};
  this._fields = attrs.fields.map(function (f) {
    var field = new Field(f, opts);
    this._fieldsMap[field.getName()] = field;
    return field;
  }, this);
  opts.namespace = namespace;

  this._isError = attrs.type === 'error';
  this._constructor = this._createConstructor();
  this._read = this._createReader();
  this._skip = this._createSkipper();
  this._write = this._createWriter();
  this._check = this._createChecker();
}
util.inherits(RecordType, Type);

RecordType.prototype._getConstructorName = function () {
  return this._name ?
    unqualify(this._name) :
    this._isError ? 'Error$' : 'Record$';
};

RecordType.prototype._createConstructor = function () {
  // jshint -W054
  var outerArgs = [];
  var innerArgs = [];
  var ds = []; // Defaults.
  var innerBody = this._isError ? '  Error.call(this);\n' : '';
  // Not calling `Error.captureStackTrace` because this wouldn't be compatible
  // with browsers other than Chrome.
  var i, l, field, name, getDefault;
  for (i = 0, l = this._fields.length; i < l; i++) {
    field = this._fields[i];
    getDefault = field.getDefault;
    name = field._name;
    innerArgs.push('v' + i);
    innerBody += '  ';
    // Addition by Ajai
    // Allow hyphens to be part of field name (including enums)
    if (getDefault() === undefined) {
      innerBody += 'this[\'' + name + '\'] = v' + i + ';\n';
    } else {
      innerBody += 'if (v' + i + ' === undefined) { ';
      innerBody += 'this[\'' + name + '\'] = d' + ds.length + '(); ';
      innerBody += '} else { this[\'' + name + '\'] = v' + i + '; }\n';
      outerArgs.push('d' + ds.length);
      ds.push(getDefault);
    }
  }
  var outerBody = 'return function ' + this._getConstructorName() + '(';
  outerBody += innerArgs.join() + ') {\n' + innerBody + '};';
  var Record = new Function(outerArgs.join(), outerBody).apply(undefined, ds);

  var self = this;
  Record.getType = function () { return self; };
  if (this._isError) {
    util.inherits(Record, Error);
    // Not setting the error's name on the prototype to be consistent with how
    // object fields are mapped to (only if defined in the schema as a field).
  }
  Record.prototype.clone = function (o) { return self.clone(this, o); };
  Record.prototype.compare = function (v) { return self.compare(this, v); };
  Record.prototype.isValid = function (o) { return self.isValid(this, o); };
  Record.prototype.toBuffer = function () { return self.toBuffer(this); };
  Record.prototype.toString = function () { return self.toString(this); };
  return Record;
};

RecordType.prototype._createChecker = function () {
  // jshint -W054
  var names = [];
  var values = [];
  var name = this._getConstructorName();
  var body = 'return function check' + name + '(v, f, h, p) {\n';
  body += '  if (\n';
  body += '    v === null ||\n';
  body += '    typeof v != \'object\' ||\n';
  body += '    (f && !this._checkFields(v))\n';
  body += '  ) {\n';
  body += '    if (h) { h(v, this); }\n';
  body += '    return false;\n';
  body += '  }\n';
  if (!this._fields.length) {
    // Special case, empty record. We handle this directly.
    body += '  return true;\n';
  } else {
    for (i = 0, l = this._fields.length; i < l; i++) {
      field = this._fields[i];
      names.push('t' + i);
      values.push(field._type);
      if (field.getDefault() !== undefined) {
        body += '  var v' + i + ' = v.' + field._name + ';\n';
      }
    }
    body += '  if (h) {\n';
    body += '    var b = 1;\n';
    body += '    var j = p.length;\n';
    body += '    p.push(\'\');\n';
    var i, l, field;
    for (i = 0, l = this._fields.length; i < l; i++) {
      field = this._fields[i];
      body += '    p[j] = \'' + field._name + '\';\n';
      body += '    b &= ';
      if (field.getDefault() === undefined) {
        body += 't' + i + '._check(v.' + field._name + ', f, h, p);\n';
      } else {
        body += 'v' + i + ' === undefined || ';
        body += 't' + i + '._check(v' + i + ', f, h, p);\n';
      }
    }
    body += '    p.pop();\n';
    body += '    return !!b;\n';
    body += '  } else {\n    return (\n      ';
    body += this._fields.map(function (field, i) {
      return field.getDefault() === undefined ?
        't' + i + '._check(v.' + field._name + ', f)' :
        '(v' + i + ' === undefined || t' + i + '._check(v' + i + ', f))';
    }).join(' &&\n      ');
    body += '\n    );\n  }\n';
  }
  body += '};';
  return new Function(names.join(), body).apply(undefined, values);
};

RecordType.prototype._createReader = function () {
  // jshint -W054
  var names = [];
  var values = [this._constructor];
  var i, l;
  for (i = 0, l = this._fields.length; i < l; i++) {
    names.push('t' + i);
    values.push(this._fields[i]._type);
  }
  var name = this._getConstructorName();
  var body = 'return function read' + name + '(t) {\n';
  body += '  return new ' + name + '(\n    ';
  body += names.map(function (s) { return s + '._read(t)'; }).join(',\n    ');
  body += '\n  );\n};';
  names.unshift(name);
  // We can do this since the JS spec guarantees that function arguments are
  // evaluated from left to right.
  return new Function(names.join(), body).apply(undefined, values);
};

RecordType.prototype._createSkipper = function () {
  // jshint -W054
  var args = [];
  var body = 'return function skip' + this._getConstructorName() + '(t) {\n';
  var values = [];
  var i, l;
  for (i = 0, l = this._fields.length; i < l; i++) {
    args.push('t' + i);
    values.push(this._fields[i]._type);
    body += '  t' + i + '._skip(t);\n';
  }
  body += '}';
  return new Function(args.join(), body).apply(undefined, values);
};

RecordType.prototype._createWriter = function () {
  // jshint -W054
  // We still do default handling here, in case a normal JS object is passed.
  var args = [];
  var name = this._getConstructorName();
  var body = 'return function write' + name + '(t, v) {\n';
  var values = [];
  var i, l, field, value;
  for (i = 0, l = this._fields.length; i < l; i++) {
    field = this._fields[i];
    args.push('t' + i);
    values.push(field._type);
    body += '  ';
    if (field.getDefault() === undefined) {
      body += 't' + i + '._write(t, v.' + field._name + ');\n';
    } else {
      value = field._type.toBuffer(field.getDefault()).toString('binary');
      // Convert the default value to a binary string ahead of time. We aren't
      // converting it to a buffer to avoid retaining too much memory. If we
      // had our own buffer pool, this could be an idea in the future.
      args.push('d' + i);
      values.push(value);
      body += 'var v' + i + ' = v.' + field._name + ';\n';
      body += 'if (v' + i + ' === undefined) {\n';
      body += '    t.writeBinary(d' + i + ', ' + value.length + ');\n';
      body += '  } else {\n    t' + i + '._write(t, v' + i + ');\n  }\n';
    }
  }
  body += '}';
  return new Function(args.join(), body).apply(undefined, values);
};

RecordType.prototype._update = function (resolver, type, opts) {
  // jshint -W054
  if (type._name && !~getAliases(this).indexOf(type._name)) {
    throw new Error(f('no alias found for %s', type._name));
  }

  var rFields = this._fields;
  var wFields = type._fields;
  var wFieldsMap = utils.toMap(wFields, function (f) { return f._name; });

  var innerArgs = []; // Arguments for reader constructor.
  var resolvers = {}; // Resolvers keyed by writer field name.
  var i, j, field, name, names, matches;
  for (i = 0; i < rFields.length; i++) {
    field = rFields[i];
    names = getAliases(field);
    matches = [];
    for (j = 0; j < names.length; j++) {
      name = names[j];
      if (wFieldsMap[name]) {
        matches.push(name);
      }
    }
    if (matches.length > 1) {
      throw new Error(f('multiple matches for %s', field.name));
    }
    if (!matches.length) {
      if (field.getDefault() === undefined) {
        throw new Error(f('no match for default-less %s', field.name));
      }
      innerArgs.push('undefined');
    } else {
      name = matches[0];
      resolvers[name] = {
        resolver: field._type.createResolver(wFieldsMap[name]._type, opts),
        name: field._name // Reader field name.
      };
      innerArgs.push(field._name);
    }
  }

  // See if we can add a bypass for unused fields at the end of the record.
  var lazyIndex = -1;
  i = wFields.length;
  while (i && resolvers[wFields[--i]._name] === undefined) {
    lazyIndex = i;
  }

  var uname = this._getConstructorName();
  var args = [uname];
  var values = [this._constructor];
  var body = '  return function read' + uname + '(t, b) {\n';
  for (i = 0; i < wFields.length; i++) {
    if (i === lazyIndex) {
      body += '  if (!b) {\n';
    }
    field = type._fields[i];
    name = field._name;
    body += (~lazyIndex && i >= lazyIndex) ? '    ' : '  ';
    if (resolvers[name] === undefined) {
      args.push('t' + i);
      values.push(field._type);
      body += 't' + i + '._skip(t);\n';
    } else {
      args.push('t' + i);
      values.push(resolvers[name].resolver);
      body += 'var ' + resolvers[name].name + ' = ';
      body += 't' + i + '._read(t);\n';
    }
  }
  if (~lazyIndex) {
    body += '  }\n';
  }
  body += '  return new ' + uname + '(' + innerArgs.join() + ');\n};';

  resolver._read = new Function(args.join(), body).apply(undefined, values);
};

RecordType.prototype._match = function (tap1, tap2) {
  var fields = this._fields;
  var i, l, field, order, type;
  for (i = 0, l = fields.length; i < l; i++) {
    field = fields[i];
    order = field._order;
    type = field._type;
    if (order) {
      order *= type._match(tap1, tap2);
      if (order) {
        return order;
      }
    } else {
      type._skip(tap1);
      type._skip(tap2);
    }
  }
  return 0;
};

RecordType.prototype._checkFields = function (obj) {
  var keys = Object.keys(obj);
  var i, l;
  for (i = 0, l = keys.length; i < l; i++) {
    if (!this._fieldsMap[keys[i]]) {
      return false;
    }
  }
  return true;
};

RecordType.prototype._copy = function (val, opts) {
  // jshint -W058
  var hook = opts && opts.fieldHook;
  var values = [undefined];
  var i, l, field, value;
  for (i = 0, l = this._fields.length; i < l; i++) {
    field = this._fields[i];
    value = val[field._name];
    if (value === undefined && field.hasOwnProperty('getDefault')) {
      value = field.getDefault();
    } else {
      value = field._type._copy(val[field._name], opts);
    }
    if (hook) {
      value = hook(field, value, this);
    }
    values.push(value);
  }
  return new (this._constructor.bind.apply(this._constructor, values));
};

RecordType.prototype.compare = function (val1, val2) {
  var fields = this._fields;
  var i, l, field, name, order, type;
  for (i = 0, l = fields.length; i < l; i++) {
    field = fields[i];
    name = field._name;
    order = field._order;
    type = field._type;
    if (order) {
      order *= type.compare(val1[name], val2[name]);
      if (order) {
        return order;
      }
    }
  }
  return 0;
};

RecordType.prototype.random = function () {
  // jshint -W058
  var fields = this._fields.map(function (f) { return f._type.random(); });
  fields.unshift(undefined);
  return new (this._constructor.bind.apply(this._constructor, fields));
};

RecordType.prototype.getAliases = function () { return this._aliases; };

RecordType.prototype.getField = function (name) {
  return this._fieldsMap[name];
};

RecordType.prototype.getFields = function () { return this._fields.slice(); };

RecordType.prototype.getRecordConstructor = function () {
  return this._constructor;
};

RecordType.prototype.getTypeName = function () {
  return this._isError ? 'error' : 'record';
};

RecordType.prototype.toJSON = function () {
  // The nested JSONification of fields isn't required for `getSchema` (it
  // would call it recursively anyway), but it makes other things simpler by
  // letting us return valid "canonical attributes" directly (e.g. meta types).
  return {
    name: this._name,
    type: this.getTypeName(),
    fields: this._fields.map(function (f) { return f.toJSON(); }),
    aliases: this._aliases
  };
};

/**
 * Derived type abstract class.
 *
 */
function LogicalType(attrs, opts) {
  this._logicalTypeName = attrs.logicalType;
  Type.call(this);
  LOGICAL_TYPE = this;
  this._underlyingType = createType(attrs, opts);
}
util.inherits(LogicalType, Type);

LogicalType.prototype.getTypeName = function () {
  return 'logical:' + this._logicalTypeName;
};

LogicalType.prototype.getUnderlyingType = function () {
  return this._underlyingType;
};

LogicalType.prototype._read = function (tap) {
  return this._fromValue(this._underlyingType._read(tap));
};

LogicalType.prototype._write = function (tap, any) {
  this._underlyingType._write(tap, this._toValue(any));
};

LogicalType.prototype._check = function (any, flags, hook, path) {
  try {
    var val = this._toValue(any);
  } catch (err) {
    // Handled below.
  }
  if (val === undefined) {
    if (hook) {
      hook(any, this);
    }
    return false;
  }
  return this._underlyingType._check(val, flags, hook, path);
};

LogicalType.prototype._copy = function (any, opts) {
  var type = this._underlyingType;
  switch (opts && opts.coerce) {
    case 3: // To string.
      return type._copy(this._toValue(any), opts);
    case 2: // From string.
      return this._fromValue(type._copy(any, opts));
    default: // Normal copy.
      return this._fromValue(type._copy(this._toValue(any), opts));
  }
};

LogicalType.prototype._update = function (resolver, type, opts) {
  var _fromValue = this._resolve(type, opts);
  if (_fromValue) {
    resolver._read = function (tap) { return _fromValue(type._read(tap)); };
  }
};

LogicalType.prototype.random = function () {
  return this._fromValue(this._underlyingType.random());
};

LogicalType.prototype.compare = function (obj1, obj2) {
  var val1 = this._toValue(obj1);
  var val2 = this._toValue(obj2);
  return this._underlyingType.compare(val1, val2);
};

LogicalType.prototype.toJSON = function () {
  var attrs = this.getUnderlyingType().toJSON();
  if (EXPORT_ATTRS) {
    if (typeof attrs == 'string') {
      attrs = {type: attrs};
    }
    attrs.logicalType = this._logicalTypeName;
    this._export(attrs);
  }
  return attrs;
};

// Unlike the other methods below, `_export` has a reasonable default which we
// can provide (not exporting anything).
LogicalType.prototype._export = function (/* attrs */) {};

// Methods to be implemented.
LogicalType.prototype._fromValue = utils.abstractFunction;
LogicalType.prototype._toValue = utils.abstractFunction;
LogicalType.prototype._resolve = utils.abstractFunction;


// General helpers.

/**
 * Customizable long.
 *
 * This allows support of arbitrarily large long (e.g. larger than
 * `Number.MAX_SAFE_INTEGER`). See `LongType.__with` method above. Note that we
 * can't use a logical type because we need a "lower-level" hook here: passing
 * through through the standard long would cause a loss of precision.
 *
 */
function AbstractLongType(noUnpack) {
  LongType.call(this);
  this._noUnpack = !!noUnpack;
}
util.inherits(AbstractLongType, LongType);

AbstractLongType.prototype._check = function (val, flags, hook) {
  var b = this._isValid(val);
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

AbstractLongType.prototype._read = function (tap) {
  var buf, pos;
  if (this._noUnpack) {
    pos = tap.pos;
    tap.skipLong();
    buf = tap.buf.slice(pos, tap.pos);
  } else {
    buf = tap.unpackLongBytes(tap);
  }
  if (tap.isValid()) {
    return this._fromBuffer(buf);
  }
};

AbstractLongType.prototype._write = function (tap, val) {
  if (!this._isValid(val)) {
    throwInvalidError(val, this);
  }
  var buf = this._toBuffer(val);
  if (this._noUnpack) {
    tap.writeFixed(buf);
  } else {
    tap.packLongBytes(buf);
  }
};

AbstractLongType.prototype._copy = function (val, opts) {
  switch (opts && opts.coerce) {
    case 3: // To string.
      return this._toJSON(val);
    case 2: // From string.
      return this._fromJSON(val);
    default: // Normal copy.
      // Slow but guarantees most consistent results. Faster alternatives would
      // require assumptions on the long class used (e.g. immutability).
      return this._fromJSON(JSON.parse(JSON.stringify(this._toJSON(val))));
  }
};

AbstractLongType.prototype.random = function () {
  return this._fromJSON(LongType.prototype.random());
};

// Methods to be implemented by the user.
AbstractLongType.prototype._fromBuffer = utils.abstractFunction;
AbstractLongType.prototype._toBuffer = utils.abstractFunction;
AbstractLongType.prototype._fromJSON = utils.abstractFunction;
AbstractLongType.prototype._toJSON = utils.abstractFunction;
AbstractLongType.prototype._isValid = utils.abstractFunction;
AbstractLongType.prototype.compare = utils.abstractFunction;

/**
 * Field.
 *
 * @param attrs {Object} The field's schema.
 * @para opts {Object} Schema parsing options (the same as `Type`s').
 *
 */
function Field(attrs, opts) {
  var name = attrs.name;
  if (typeof name != 'string' || !isValidName(name)) {
    throw new Error(f('invalid field name: %s', name));
  }

  this._name = name;
  this._type = createType(attrs.type, opts);
  this._aliases = attrs.aliases || [];

  this._order = (function (order) {
    switch (order) {
      case 'ascending':
        return 1;
      case 'descending':
        return -1;
      case 'ignore':
        return 0;
      default:
        throw new Error(f('invalid order: %j', order));
    }
  })(attrs.order === undefined ? 'ascending' : attrs.order);

  var value = attrs['default'];
  if (value !== undefined) {
    // We need to convert defaults back to a valid format (unions are
    // disallowed in default definitions, only the first type of each union is
    // allowed instead).
    // http://apache-avro.679487.n3.nabble.com/field-union-default-in-Java-td1175327.html
    var type = this._type;
    var val = type._copy(value, {coerce: 2, wrap: 2});
    // The clone call above will throw an error if the default is invalid.
    if (isPrimitive(type.getTypeName()) && type.getTypeName() !== 'bytes') {
      // These are immutable.
      this.getDefault = function () { return val; };
    } else {
      this.getDefault = function () { return type._copy(val); };
    }
  }
}

Field.prototype.getAliases = function () { return this._aliases; };

Field.prototype.getDefault = function () {}; // Undefined default.

Field.prototype.getName = function () { return this._name; };

Field.prototype.getOrder = function () {
  return ['descending', 'ignore', 'ascending'][this._order + 1];
};

Field.prototype.getType = function () { return this._type; };

Field.prototype.toJSON = function () {
  var val = this.getDefault();
  if (val !== undefined) {
    // We must both unwrap all unions and coerce buffers to strings.
    val = this._type._copy(val, {coerce: 3, wrap: 3});
  }
  return {
    name: this._name,
    type: this._type,
    'default': val,
    order: this.getOrder(),
    aliases: this._aliases
  };
};

Field.prototype.inspect = Field.prototype.toJSON;

/**
 * Resolver to read a writer's schema as a new schema.
 *
 * @param readerType {Type} The type to convert to.
 *
 */
function Resolver(readerType) {
  // Add all fields here so that all resolvers share the same hidden class.
  this._readerType = readerType;
  this._items = null;
  this._read = null;
  this._size = 0;
  this._symbols = null;
  this._values = null;
}

Resolver.prototype.inspect = function () { return '<Resolver>'; };

/**
 * Read a value from a tap.
 *
 * @param type {Type} The type to decode.
 * @param tap {Tap} The tap to read from. No checks are performed here.
 * @param resolver {Resolver} Optional resolver. It must match the input type.
 * @param lazy {Boolean} Skip trailing fields when using a resolver.
 *
 */
function readValue(type, tap, resolver, lazy) {
  if (resolver) {
    if (resolver._readerType !== type) {
      throw new Error('invalid resolver');
    }
    return resolver._read(tap, lazy);
  } else {
    return type._read(tap);
  }
}

/**
 * Remove namespace from a name.
 *
 * @param name {String} Full or short name.
 *
 */
function unqualify(name) {
  var parts = name.split('.');
  return parts[parts.length - 1];
}

/**
 * Verify and return fully qualified name.
 *
 * @param name {String} Full or short name. It can be prefixed with a dot to
 * force global namespace.
 * @param namespace {String} Optional namespace.
 *
 */
function qualify(name, namespace) {
  if (~name.indexOf('.')) {
    name = name.replace(/^\./, ''); // Allow absolute referencing.
  } else if (namespace) {
    name = namespace + '.' + name;
  }
  name.split('.').forEach(function (part) {
    if (!isValidName(part)) {
      throw new Error(f('invalid name: %j', name));
    }
  });
  var tail = unqualify(name);
  // Primitives are always in the global namespace.
  return isPrimitive(tail) ? tail : name;
}

/**
 * Get all aliases for a type (including its name).
 *
 * @param obj {Type|Object} Typically a type or a field. Its aliases property
 * must exist and be an array.
 *
 */
function getAliases(obj) {
  var names = [];
  if (obj._name) {
    names.push(obj._name);
  }
  var aliases = obj._aliases;
  var i, l;
  for (i = 0, l = aliases.length; i < l; i++) {
    names.push(aliases[i]);
  }
  return names;
}

/**
 * Check whether a type's name is a primitive.
 *
 * @param name {String} Type name (e.g. `'string'`, `'array'`).
 *
 */
function isPrimitive(typeName) {
  // Since we use this module's own `TYPES` object, we can use `instanceof`.
  var type = TYPES[typeName];
  return type && type.prototype instanceof PrimitiveType;
}

/**
 * Return a type's class name from its Avro type name.
 *
 * We can't simply use `constructor.name` since it isn't supported in all
 * browsers.
 *
 * @param typeName {String} Type name.
 *
 */
function getClassName(typeName) {
  if (typeName === 'error') {
    typeName = 'record';
  } else {
    var match = /^([^:]+):(.*)$/.exec(typeName);
    if (match) {
      if (match[1] === 'union') {
        typeName = match[2] + 'Union';
      } else {
        // Logical type.
        typeName = match[1];
      }
    }
  }
  return utils.capitalize(typeName) + 'Type';
}

/**
 * Get the number of elements in an array block.
 *
 * @param tap {Tap} A tap positioned at the beginning of an array block.
 *
 */
function readArraySize(tap) {
  var n = tap.readLong();
  if (n < 0) {
    n = -n;
    tap.skipLong(); // Skip size.
  }
  return n;
}

/**
 * Correctly stringify an object which contains types.
 *
 * @param obj {Object} The object to stringify. Typically, a type itself or an
 * object containing types. Any types inside will be expanded only once then
 * referenced by name.
 * @param opts {Object} Options:
 *  + `exportAttrs` {Boolean} Include field and logical type attributes.
 *  + `noDeref` {Boolean} Always reference types by name when possible,
 *    rather than expand it the first time it is encountered.
 *
 */
function stringify(obj, opts) {
  EXPORT_ATTRS = opts && opts.exportAttrs;
  var noDeref = opts && opts.noDeref;

  // Since JS objects are unordered, this implementation (unfortunately)
  // relies on engines returning properties in the same order that they are
  // inserted in. This is not in the JS spec, but can be "somewhat" safely
  // assumed (more here: http://stackoverflow.com/q/5525795/1062617).
  return (function (registry) {
    return JSON.stringify(obj, function (key, value) {
      if (value) {
        if (
          typeof value == 'object' &&
          value.hasOwnProperty('default') &&
          !value.hasOwnProperty('logicalType')
        ) {
          // This is a field.
          if (EXPORT_ATTRS) {
            return {
              name: value.name,
              type: value.type,
              'default': value['default'],
              order: value.order !== 'ascending' ? value.order : undefined,
              aliases: value.aliases.length ? value.aliases : undefined
            };
          } else {
            return {name: value.name, type: value.type};
          }
        } else if (value.aliases) {
          // This is a named type (enum, fixed, record, error).
          var name = value.name;
          if (name) {
            // If the type is anonymous, we always dereference it.
            if (noDeref || registry[name]) {
              return name;
            }
            registry[name] = true;
          }
          if (!EXPORT_ATTRS || !value.aliases.length) {
            value.aliases = undefined;
          }
        }
      }
      return value;
    });
  })({});
}

/**
 * Check whether a long can be represented without precision loss.
 *
 * @param n {Number} The number.
 *
 * Two things to note:
 *
 * + We are not using the `Number` constants for compatibility with older
 *   browsers.
 * + We must remove one from each bound because of rounding errors.
 *
 */
function isSafeLong(n) {
  return n >= -9007199254740990 && n <= 9007199254740990;
}

/**
 * Check whether an object is the JSON representation of a buffer.
 *
 */
function isJsonBuffer(obj) {
  return obj && obj.type === 'Buffer' && Array.isArray(obj.data);
}

/**
 * Check whether a string is a valid Avro identifier.
 *
 */
function isValidName(str) { return NAME_PATTERN.test(str); }

/**
 * Throw a somewhat helpful error on invalid object.
 *
 * @param path {Array} Passed from hook, but unused (because empty where this
 * function is used, since we aren't keeping track of it for effiency).
 * @param val {...} The object to reject.
 * @param type {Type} The type to check against.
 *
 * This method is mostly used from `_write` to signal an invalid object for a
 * given type. Note that this provides less information than calling `isValid`
 * with a hook since the path is not propagated (for efficiency reasons).
 *
 */
function throwInvalidError(val, type) {
  throw new Error(f('invalid %s: %j', type, val));
}

/**
 * Get a type's bucket when included inside an unwrapped union.
 *
 * @param type {Type} Any type.
 *
 */
function getTypeBucket(type) {
  var typeName = type.getTypeName();
  switch (typeName) {
    case 'double':
    case 'float':
    case 'int':
    case 'long':
      return 'number';
    case 'bytes':
    case 'fixed':
      return 'buffer';
    case 'enum':
      return 'string';
    case 'map':
    case 'error':
    case 'record':
      return 'object';
    default:
      return typeName;
  }
}

/**
 * Infer a value's bucket (see unwrapped unions for more details).
 *
 * @param val {...} Any value.
 *
 */
function getValueBucket(val) {
  if (val === null) {
    return 'null';
  }
  var bucket = typeof val;
  if (bucket === 'object') {
    // Could be bytes, fixed, array, map, or record.
    if (Array.isArray(val)) {
      return 'array';
    } else if (Buffer.isBuffer(val)) {
      return 'buffer';
    }
  }
  return bucket;
}


module.exports = {
  Type: Type,
  createType: createType,
  getTypeBucket: getTypeBucket,
  getValueBucket: getValueBucket,
  isValidName: isValidName,
  stringify: stringify,
  builtins: (function () {
    var types = {
      LogicalType: LogicalType,
      UnwrappedUnionType: UnwrappedUnionType,
      WrappedUnionType: WrappedUnionType
    };
    var typeNames = Object.keys(TYPES);
    var i, l, typeName;
    for (i = 0, l = typeNames.length; i < l; i++) {
      typeName = typeNames[i];
      types[getClassName(typeName)] = TYPES[typeName];
    }
    return types;
  })()
};

}).call(this,require("buffer").Buffer)
},{"./utils":35,"buffer":4,"util":28}],35:[function(require,module,exports){
(function (Buffer){
/* jshint node: true */

// TODO: Make long comparison impervious to precision loss.
// TODO: Optimize binary comparison methods.

'use strict';

/**
 * Various utilities used across this library.
 *
 */

var crypto = require('crypto');


/**
 * Uppercase the first letter of a string.
 *
 * @param s {String} The string.
 *
 */
function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

/**
 * Compare two numbers.
 *
 * @param n1 {Number} The first one.
 * @param n2 {Number} The second one.
 *
 */
function compare(n1, n2) { return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1); }

/**
 * Compute a string's hash.
 *
 * @param str {String} The string to hash.
 * @param algorithm {String} The algorithm used. Defaults to MD5.
 *
 */
function getHash(str, algorithm) {
  algorithm = algorithm || 'md5';
  var hash = crypto.createHash(algorithm);
  hash.end(str);
  return hash.read();
}

/**
 * Find index of value in array.
 *
 * @param arr {Array} Can also be a false-ish value.
 * @param v {Object} Value to find.
 *
 * Returns -1 if not found, -2 if found multiple times.
 *
 */
function singleIndexOf(arr, v) {
  var pos = -1;
  var i, l;
  if (!arr) {
    return -1;
  }
  for (i = 0, l = arr.length; i < l; i++) {
    if (arr[i] === v) {
      if (pos >= 0) {
        return -2;
      }
      pos = i;
    }
  }
  return pos;
}

/**
 * Convert array to map.
 *
 * @param arr {Array} Elements.
 * @param fn {Function} Function returning an element's key.
 *
 */
function toMap(arr, fn) {
  var obj = {};
  var i, elem;
  for (i = 0; i < arr.length; i++) {
    elem = arr[i];
    obj[fn(elem)] = elem;
  }
  return obj;
}

/**
 * Check whether an array has duplicates.
 *
 * @param arr {Array} The array.
 * @param fn {Function} Optional function to apply to each element.
 *
 */
function hasDuplicates(arr, fn) {
  var obj = {};
  var i, l, elem;
  for (i = 0, l = arr.length; i < l; i++) {
    elem = arr[i];
    if (fn) {
      elem = fn(elem);
    }
    if (obj[elem]) {
      return true;
    }
    obj[elem] = true;
  }
  return false;
}

/**
 * Returns offset in the string of the end of JSON object (-1 if past the end).
 *
 * To keep the implementation simple, this function isn't a JSON validator. It
 * will gladly return a result for invalid JSON (which is OK since that will be
 * promptly rejected by the JSON parser). What matters is that it is guaranteed
 * to return the correct end when presented with valid JSON.
 *
 * @param str {String} Input string containing serialized JSON..
 * @param pos {Number} Starting position.
 *
 */
function jsonEnd(str, pos) {
  pos = pos | 0;

  // Handle the case of a simple literal separately.
  var c = str.charAt(pos++);
  if (/[\d-]/.test(c)) {
    while (/[eE\d.+-]/.test(str.charAt(pos))) {
      pos++;
    }
    return pos;
  } else if (/true|null/.test(str.slice(pos - 1, pos + 3))) {
    return pos + 3;
  } else if (/false/.test(str.slice(pos - 1, pos + 4))) {
    return pos + 4;
  }

  // String, object, or array.
  var depth = 0;
  var literal = false;
  do {
    switch (c) {
    case '{':
    case '[':
      if (!literal) { depth++; }
      break;
    case '}':
    case ']':
      if (!literal && !--depth) {
        return pos;
      }
      break;
    case '"':
      literal = !literal;
      if (!depth && !literal) {
        return pos;
      }
      break;
    case '\\':
      pos++; // Skip the next character.
    }
  } while ((c = str.charAt(pos++)));

  return -1;
}

/**
 * "Abstract" function to help with "subclassing".
 *
 */
function abstractFunction() { throw new Error('abstract'); }

/**
 * Generator of random things.
 *
 * Inspired by: http://stackoverflow.com/a/424445/1062617
 *
 */
function Lcg(seed) {
  var a = 1103515245;
  var c = 12345;
  var m = Math.pow(2, 31);
  var state = Math.floor(seed || Math.random() * (m - 1));

  this._max = m;
  this._nextInt = function () { return state = (a * state + c) % m; };
}

Lcg.prototype.nextBoolean = function () {
  // jshint -W018
  return !!(this._nextInt() % 2);
};

Lcg.prototype.nextInt = function (start, end) {
  if (end === undefined) {
    end = start;
    start = 0;
  }
  end = end === undefined ? this._max : end;
  return start + Math.floor(this.nextFloat() * (end - start));
};

Lcg.prototype.nextFloat = function (start, end) {
  if (end === undefined) {
    end = start;
    start = 0;
  }
  end = end === undefined ? 1 : end;
  return start + (end - start) * this._nextInt() / this._max;
};

Lcg.prototype.nextString = function(len, flags) {
  len |= 0;
  flags = flags || 'aA';
  var mask = '';
  if (flags.indexOf('a') > -1) {
    mask += 'abcdefghijklmnopqrstuvwxyz';
  }
  if (flags.indexOf('A') > -1) {
    mask += 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  }
  if (flags.indexOf('#') > -1) {
    mask += '0123456789';
  }
  if (flags.indexOf('!') > -1) {
    mask += '~`!@#$%^&*()_+-={}[]:";\'<>?,./|\\';
  }
  var result = [];
  for (var i = 0; i < len; i++) {
    result.push(this.choice(mask));
  }
  return result.join('');
};

Lcg.prototype.nextBuffer = function (len) {
  var arr = [];
  var i;
  for (i = 0; i < len; i++) {
    arr.push(this.nextInt(256));
  }
  return new Buffer(arr);
};

Lcg.prototype.choice = function (arr) {
  var len = arr.length;
  if (!len) {
    throw new Error('choosing from empty array');
  }
  return arr[this.nextInt(len)];
};

/**
 * Ordered queue which returns items consecutively.
 *
 * This is actually a heap by index, with the added requirements that elements
 * can only be retrieved consecutively.
 *
 */
function OrderedQueue() {
  this._index = 0;
  this._items = [];
}

OrderedQueue.prototype.push = function (item) {
  var items = this._items;
  var i = items.length | 0;
  var j;
  items.push(item);
  while (i > 0 && items[i].index < items[j = ((i - 1) >> 1)].index) {
    item = items[i];
    items[i] = items[j];
    items[j] = item;
    i = j;
  }
};

OrderedQueue.prototype.pop = function () {
  var items = this._items;
  var len = (items.length - 1) | 0;
  var first = items[0];
  if (!first || first.index > this._index) {
    return null;
  }
  this._index++;
  if (!len) {
    items.pop();
    return first;
  }
  items[0] = items.pop();
  var mid = len >> 1;
  var i = 0;
  var i1, i2, j, item, c, c1, c2;
  while (i < mid) {
    item = items[i];
    i1 = (i << 1) + 1;
    i2 = (i + 1) << 1;
    c1 = items[i1];
    c2 = items[i2];
    if (!c2 || c1.index <= c2.index) {
      c = c1;
      j = i1;
    } else {
      c = c2;
      j = i2;
    }
    if (c.index >= item.index) {
      break;
    }
    items[j] = item;
    items[i] = c;
    i = j;
  }
  return first;
};

/**
 * A tap is a buffer which remembers what has been already read.
 *
 * It is optimized for performance, at the cost of failing silently when
 * overflowing the buffer. This is a purposeful trade-off given the expected
 * rarity of this case and the large performance hit necessary to enforce
 * validity. See `isValid` below for more information.
 *
 */
function Tap(buf, pos) {
  this.buf = buf;
  this.pos = pos | 0;
  if (this.pos < 0) {
    throw new Error('negative offset');
  }
}

/**
 * Check that the tap is in a valid state.
 *
 * For efficiency reasons, none of the methods below will fail if an overflow
 * occurs (either read, skip, or write). For this reason, it is up to the
 * caller to always check that the read, skip, or write was valid by calling
 * this method.
 *
 */
Tap.prototype.isValid = function () { return this.pos <= this.buf.length; };

// Read, skip, write methods.
//
// These should fail silently when the buffer overflows. Note this is only
// required to be true when the functions are decoding valid objects. For
// example errors will still be thrown if a bad count is read, leading to a
// negative position offset (which will typically cause a failure in
// `readFixed`).

Tap.prototype.readBoolean = function () { return !!this.buf[this.pos++]; };

Tap.prototype.skipBoolean = function () { this.pos++; };

Tap.prototype.writeBoolean = function (b) { this.buf[this.pos++] = !!b; };

Tap.prototype.readInt = Tap.prototype.readLong = function () {
  var n = 0;
  var k = 0;
  var buf = this.buf;
  var b, h, f, fk;

  do {
    b = buf[this.pos++];
    h = b & 0x80;
    n |= (b & 0x7f) << k;
    k += 7;
  } while (h && k < 28);

  if (h) {
    // Switch to float arithmetic, otherwise we might overflow.
    f = n;
    fk = 268435456; // 2 ** 28.
    do {
      b = buf[this.pos++];
      f += (b & 0x7f) * fk;
      fk *= 128;
    } while (b & 0x80);
    return (f % 2 ? -(f + 1) : f) / 2;
  }

  return (n >> 1) ^ -(n & 1);
};

Tap.prototype.skipInt = Tap.prototype.skipLong = function () {
  var buf = this.buf;
  while (buf[this.pos++] & 0x80) {}
};

Tap.prototype.writeInt = Tap.prototype.writeLong = function (n) {
  var buf = this.buf;
  var f, m;

  if (n >= -1073741824 && n < 1073741824) {
    // Won't overflow, we can use integer arithmetic.
    m = n >= 0 ? n << 1 : (~n << 1) | 1;
    do {
      buf[this.pos] = m & 0x7f;
      m >>= 7;
    } while (m && (buf[this.pos++] |= 0x80));
  } else {
    // We have to use slower floating arithmetic.
    f = n >= 0 ? n * 2 : (-n * 2) - 1;
    do {
      buf[this.pos] = f & 0x7f;
      f /= 128;
    } while (f >= 1 && (buf[this.pos++] |= 0x80));
  }
  this.pos++;
};

Tap.prototype.readFloat = function () {
  var buf = this.buf;
  var pos = this.pos;
  this.pos += 4;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.readFloatLE(pos);
};

Tap.prototype.skipFloat = function () { this.pos += 4; };

Tap.prototype.writeFloat = function (f) {
  var buf = this.buf;
  var pos = this.pos;
  this.pos += 4;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.writeFloatLE(f, pos);
};

Tap.prototype.readDouble = function () {
  var buf = this.buf;
  var pos = this.pos;
  this.pos += 8;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.readDoubleLE(pos);
};

Tap.prototype.skipDouble = function () { this.pos += 8; };

Tap.prototype.writeDouble = function (d) {
  var buf = this.buf;
  var pos = this.pos;
  this.pos += 8;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.writeDoubleLE(d, pos);
};

Tap.prototype.readFixed = function (len) {
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  var fixed = new Buffer(len);
  this.buf.copy(fixed, 0, pos, pos + len);
  return fixed;
};

Tap.prototype.skipFixed = function (len) { this.pos += len; };

Tap.prototype.writeFixed = function (buf, len) {
  len = len || buf.length;
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  buf.copy(this.buf, pos, 0, len);
};

Tap.prototype.readBytes = function () {
  return this.readFixed(this.readLong());
};

Tap.prototype.skipBytes = function () {
  var len = this.readLong();
  this.pos += len;
};

Tap.prototype.writeBytes = function (buf) {
  var len = buf.length;
  this.writeLong(len);
  this.writeFixed(buf, len);
};

Tap.prototype.readString = function () {
  var len = this.readLong();
  var pos = this.pos;
  var buf = this.buf;
  this.pos += len;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.utf8Slice(pos, pos + len);
};

Tap.prototype.skipString = function () {
  var len = this.readLong();
  this.pos += len;
};

Tap.prototype.writeString = function (s) {
  var len = Buffer.byteLength(s);
  this.writeLong(len);
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  this.buf.utf8Write(s, pos, len);
};

// Helper used to speed up writing defaults.

Tap.prototype.writeBinary = function (str, len) {
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  this.buf.binaryWrite(str, pos, len);
};

// Binary comparison methods.
//
// These are not guaranteed to consume the objects they are comparing when
// returning a non-zero result (allowing for performance benefits), so no other
// operations should be done on either tap after a compare returns a non-zero
// value. Also, these methods do not have the same silent failure requirement
// as read, skip, and write since they are assumed to be called on valid
// buffers.

Tap.prototype.matchBoolean = function (tap) {
  return this.buf[this.pos++] - tap.buf[tap.pos++];
};

Tap.prototype.matchInt = Tap.prototype.matchLong = function (tap) {
  var n1 = this.readLong();
  var n2 = tap.readLong();
  return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
};

Tap.prototype.matchFloat = function (tap) {
  var n1 = this.readFloat();
  var n2 = tap.readFloat();
  return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
};

Tap.prototype.matchDouble = function (tap) {
  var n1 = this.readDouble();
  var n2 = tap.readDouble();
  return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
};

Tap.prototype.matchFixed = function (tap, len) {
  return this.readFixed(len).compare(tap.readFixed(len));
};

Tap.prototype.matchBytes = Tap.prototype.matchString = function (tap) {
  var l1 = this.readLong();
  var p1 = this.pos;
  this.pos += l1;
  var l2 = tap.readLong();
  var p2 = tap.pos;
  tap.pos += l2;
  var b1 = this.buf.slice(p1, this.pos);
  var b2 = tap.buf.slice(p2, tap.pos);
  return b1.compare(b2);
};

// Functions for supporting custom long classes.
//
// The two following methods allow the long implementations to not have to
// worry about Avro's zigzag encoding, we directly expose longs as unpacked.

Tap.prototype.unpackLongBytes = function () {
  var res = new Buffer(8);
  var n = 0;
  var i = 0; // Byte index in target buffer.
  var j = 6; // Bit offset in current target buffer byte.
  var buf = this.buf;
  var b, neg;

  b = buf[this.pos++];
  neg = b & 1;
  res.fill(0);

  n |= (b & 0x7f) >> 1;
  while (b & 0x80) {
    b = buf[this.pos++];
    n |= (b & 0x7f) << j;
    j += 7;
    if (j >= 8) {
      // Flush byte.
      j -= 8;
      res[i++] = n;
      n >>= 8;
    }
  }
  res[i] = n;

  if (neg) {
    invert(res, 8);
  }

  return res;
};

Tap.prototype.packLongBytes = function (buf) {
  var neg = (buf[7] & 0x80) >> 7;
  var res = this.buf;
  var j = 1;
  var k = 0;
  var m = 3;
  var n;

  if (neg) {
    invert(buf, 8);
    n = 1;
  } else {
    n = 0;
  }

  var parts = [
    buf.readUIntLE(0, 3),
    buf.readUIntLE(3, 3),
    buf.readUIntLE(6, 2)
  ];
  // Not reading more than 24 bits because we need to be able to combine the
  // "carry" bits from the previous part and JavaScript only supports bitwise
  // operations on 32 bit integers.
  while (m && !parts[--m]) {} // Skip trailing 0s.

  // Leading parts (if any), we never bail early here since we need the
  // continuation bit to be set.
  while (k < m) {
    n |= parts[k++] << j;
    j += 24;
    while (j > 7) {
      res[this.pos++] = (n & 0x7f) | 0x80;
      n >>= 7;
      j -= 7;
    }
  }

  // Final part, similar to normal packing aside from the initial offset.
  n |= parts[m] << j;
  do {
    res[this.pos] = n & 0x7f;
    n >>= 7;
  } while (n && (res[this.pos++] |= 0x80));
  this.pos++;

  // Restore original buffer (could make this optional?).
  if (neg) {
    invert(buf, 8);
  }
};

// Helpers.

/**
 * Invert all bits in a buffer.
 *
 * @param buf {Buffer} Non-empty buffer to invert.
 * @param len {Number} Buffer length (must be positive).
 *
 */
function invert(buf, len) {
  while (len--) {
    buf[len] = ~buf[len];
  }
}


module.exports = {
  abstractFunction: abstractFunction,
  capitalize: capitalize,
  getHash: getHash,
  compare: compare,
  jsonEnd: jsonEnd,
  toMap: toMap,
  singleIndexOf: singleIndexOf,
  hasDuplicates: hasDuplicates,
  Lcg: Lcg,
  OrderedQueue: OrderedQueue,
  Tap: Tap
};

}).call(this,require("buffer").Buffer)
},{"buffer":4,"crypto":30}],36:[function(require,module,exports){
(function (Buffer){
/* jshint node: true */

'use strict';

// TODO: Add a few built-in logical types to help inference (e.g. dates)?

/**
 * From values to types.
 *
 * This file also contains the (very related) logic to combine types.
 *
 */

var types = require('./types'),
    utils = require('./utils');


// Convenience imports.
var f = utils.format;
var createType = types.createType;
var getTypeBucket = types.getTypeBucket;

// Placeholder type used when inferring the type of an empty array.
var EMPTY_ARRAY_TYPE = createType({type: 'array', items: 'null'});

/**
 * Infer a type from a value.
 *
 */
function infer(val, opts) {
  opts = opts || {};

  // Optional custom inference hook.
  if (opts.valueHook) {
    var type = opts.valueHook(val, opts);
    if (type !== undefined) {
      if (!types.Type.isType(type)) {
        throw new Error(f('invalid value hook return value: %j', type));
      }
      return type;
    }
  }

  // Default inference logic.
  switch (typeof val) {
    case 'string':
      return createType('string', opts);
    case 'boolean':
      return createType('boolean', opts);
    case 'number':
      if ((val | 0) === val) {
        return createType('int', opts);
      } else if (Math.abs(val) < 9007199254740991) {
        return createType('float', opts);
      }
      return createType('double', opts);
    case 'object':
      if (val === null) {
        return createType('null', opts);
      } else if (Array.isArray(val)) {
        if (!val.length) {
          return EMPTY_ARRAY_TYPE;
        }
        return createType({
          type: 'array',
          items: combine(val.map(function (v) { return infer(v, opts); }))
        }, opts);
      } else if (Buffer.isBuffer(val)) {
        return createType('bytes', opts);
      }
      var fieldNames = Object.keys(val);
      if (fieldNames.some(function (s) { return !types.isValidName(s); })) {
        // We have to fall back to a map.
        return createType({
          type: 'map',
          values: combine(fieldNames.map(function (s) {
            return infer(val[s], opts);
          }), opts)
        }, opts);
      }
      return createType({
        type: 'record',
        fields: fieldNames.map(function (s) {
          return {name: s, type: infer(val[s], opts)};
        })
      }, opts);
    default:
      throw new Error(f('cannot infer type from: %j', val));
  }
}

/**
 * Combine types into one.
 *
 */
function combine(types, opts) {
  if (!types.length) {
    throw new Error('no types to combine');
  }
  if (types.length === 1) {
    return types[0]; // Nothing to do.
  }

  // Extract any union types.
  var expanded = [];
  types.forEach(function (type) {
    switch (type.getTypeName()) {
      case 'union:wrapped':
        throw new Error('wrapped unions cannot be combined');
      case 'union:unwrapped':
        expanded = expanded.concat(type.getTypes());
        break;
      default:
        expanded.push(type);
    }
  });

  // Group types by category, similar to the logic for unwrapped unions.
  var bucketized = {};
  expanded.forEach(function (type) {
    var bucket = getTypeBucket(type);
    var bucketTypes = bucketized[bucket];
    if (!bucketTypes) {
      bucketized[bucket] = bucketTypes = [];
    }
    bucketTypes.push(type);
  });

  // Generate the "augmented" type for each group.
  var buckets = Object.keys(bucketized);
  var augmented = buckets.map(function (bucket) {
    var bucketTypes = bucketized[bucket];
    if (bucketTypes.length === 1) {
      return bucketTypes[0];
    } else {
      switch (bucket) {
        case 'null':
        case 'boolean':
          return bucketTypes[0];
        case 'number':
          return combineNumbers(bucketTypes);
        case 'string':
          return combineStrings(bucketTypes, opts);
        case 'buffer':
          return combineBuffers(bucketTypes, opts);
        case 'array':
          // Remove any sentinel arrays (used when inferring from empty arrays)
          // to avoid making things nullable when they shouldn't be.
          bucketTypes = bucketTypes.filter(function (t) {
            return t !== EMPTY_ARRAY_TYPE;
          });
          if (!bucketTypes.length) {
            // We still don't have a real type, just return the sentinel.
            return EMPTY_ARRAY_TYPE;
          }
          return createType({
            type: 'array',
            items: combine(bucketTypes.map(function (t) {
              return t.getItemsType();
            }))
          }, opts);
        default:
          return combineObjects(bucketTypes, opts);
      }
    }
  });

  if (augmented.length === 1) {
    return augmented[0];
  } else {
    // We return an (unwrapped) union of all augmented types.
    return createType(augmented, opts);
  }
}

/**
 * Combine number types.
 *
 * Note that never have to create a new type here, we are guaranteed to be able
 * to reuse one of the input types as super-type.
 *
 */
function combineNumbers(types) {
  var typeNames = ['int', 'long', 'float', 'double'];
  var superIndex = -1;
  var superType = null;
  var i, l, type, index;
  for (i = 0, l = types.length; i < l; i++) {
    type = types[i];
    index = typeNames.indexOf(type.getTypeName());
    if (index > superIndex) {
      superIndex = index;
      superType = type;
    }
  }
  return superType;
}

/**
 * Combine enums and strings.
 *
 * The order of the returned symbols is undefined and the returned enum is
 * anonymous.
 *
 */
function combineStrings(types, opts) {
  var symbols = {};
  var i, l, type, typeSymbols;
  for (i = 0, l = types.length; i < l; i++) {
    type = types[i];
    if (type.getTypeName() === 'string') {
      // If at least one of the types is a string, it will be the supertype.
      return type;
    }
    typeSymbols = type.getSymbols();
    var j, m;
    for (j = 0, m = typeSymbols.length; j < m; j++) {
      symbols[typeSymbols[j]] = true;
    }
  }
  return createType({type: 'enum', symbols: Object.keys(symbols)}, opts);
}

/**
 * Combine bytes and fixed.
 *
 * This function is optimized to avoid creating new types when possible: in
 * case of a size mismatch between fixed types, it will continue looking
 * through the array to find an existing bytes type (rather than exit early by
 * creating one eagerly).
 *
 */
function combineBuffers(types, opts) {
  var size = -1;
  var i, l, type;
  for (i = 0, l = types.length; i < l; i++) {
    type = types[i];
    if (type.getTypeName() === 'bytes') {
      return type;
    }
    if (size === -1) {
      size = type.getSize();
    } else if (type.getSize() !== size) {
      // Don't create a bytes type right away, we might be able to reuse one
      // later on in the types array. Just mark this for now.
      size = -2;
    }
  }
  return size < 0 ? createType('bytes', opts) : types[0];
}

/**
 * Combine maps and records.
 *
 * Field defaults are kept when possible (i.e. when no coercion to a map
 * happens), with later definitions overriding previous ones.
 *
 */
function combineObjects(types, opts) {
  opts = opts || {};

  var allTypes = []; // Field and value types.
  var fieldTypes = {}; // Record field types grouped by field name.
  var fieldDefaults = {};
  var isValidRecord = true;

  // Check whether the final type will be a map or a record.
  var i, l, type, fields;
  for (i = 0, l = types.length; i < l; i++) {
    type = types[i];
    if (type.getTypeName() === 'map') {
      isValidRecord = false;
      allTypes.push(type.getValuesType());
    } else {
      fields = type.getFields();
      var j, m, field, fieldDefault, fieldName, fieldType;
      for (j = 0, m = fields.length; j < m; j++) {
        field = fields[j];
        fieldName = field.getName();
        fieldType = field.getType();
        allTypes.push(fieldType);
        if (isValidRecord) {
          if (!fieldTypes[fieldName]) {
            fieldTypes[fieldName] = [];
          }
          fieldTypes[fieldName].push(fieldType);
          fieldDefault = field.getDefault();
          if (fieldDefault !== undefined) {
            // Later defaults will override any previous ones.
            fieldDefaults[fieldName] = fieldDefault;
          }
        }
      }
    }
  }

  if (isValidRecord) {
    // Check that no fields are missing and that we have the approriate
    // defaults for those which are.
    var fieldNames = Object.keys(fieldTypes);
    for (i = 0, l = fieldNames.length; i < l; i++) {
      fieldName = fieldNames[i];
      if (
        fieldTypes[fieldName].length < types.length &&
        fieldDefaults[fieldName] === undefined
      ) {
        // At least one of the records is missing a field with no default.
        if (opts && opts.strictDefaults) {
          isValidRecord = false;
        } else {
          fieldTypes[fieldName].unshift(createType('null', opts));
          fieldDefaults[fieldName] = null;
        }
      }
    }
  }

  var attrs;
  if (isValidRecord) {
    attrs = {
      type: 'record',
      fields: fieldNames.map(function (s) {
        var fieldType = combine(fieldTypes[s], opts);
        var fieldDefault = fieldDefaults[s];
        if (
          fieldDefault !== undefined &&
          ~fieldType.getTypeName().indexOf('union')
        ) {
          // Ensure that the default's corresponding type is first.
          var unionTypes = fieldType.getTypes();
          var i, l;
          for (i = 0, l = unionTypes.length; i < l; i++) {
            if (unionTypes[i].isValid(fieldDefault)) {
              break;
            }
          }
          if (i > 0) {
            var unionType = unionTypes[0];
            unionTypes[0] = unionTypes[i];
            unionTypes[i] = unionType;
            fieldType = createType(unionTypes, opts);
          }
        }
        return {
          name: s,
          type: fieldType,
          'default': fieldDefaults[s]
        };
      })
    };
  } else {
    attrs = {
      type: 'map',
      values: combine(allTypes, opts)
    };
  }
  return createType(attrs, opts);
}


module.exports = {
  combine: combine,
  infer: infer
};

}).call(this,{"isBuffer":require("../../../.nvm/versions/node/v0.12.0/lib/node_modules/browserify/node_modules/is-buffer/index.js")})
},{"../../../.nvm/versions/node/v0.12.0/lib/node_modules/browserify/node_modules/is-buffer/index.js":9,"./types":34,"./utils":35}]},{},[29])(29)
});
