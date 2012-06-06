path = require 'path'


module.exports =
  # Defines a lazy-loading property on an object that memoizes after the first call.
  lazyLoad: (object, property, getter) ->
    Object.defineProperty object, property,
      enumerable: true
      configurable: true # We need to be able to write over it
      get: ->
        result = getter()
        Object.defineProperty object, property, enumerable: true, value: result

        result

  # Regex to identify a file's path in a line of backtrace.
  STACK_PATH_EXTRACTOR: /\((.+)\:\d+\:\d+\)/

  # Helper to allow autorequire calls to support the same kind of relative pathing that require
  # performs.
  #
  # There has to be a better way of doing this than parsing a stack trace, though.
  #
  # The offset indicates how many calls we should go back in the stack to find a caller.  0 is the
  # function that is calling `getCallingDirectoryFromStack`.
  #
  # Passing __dirname from the caller is something to avoid in order to provide a more consistent
  # interface with require().
  getCallingDirectoryFromStack: (offset = 1) ->
    stackLines = new Error().stack.split "\n"

    # first line is the exception, second line is this method
    match = stackLines[offset + 2].match @STACK_PATH_EXTRACTOR

    # Fall back to the current directory if we don't have a valid caller.  They're likely in a REPL.
    (match and path.dirname match[1]) or process.cwd()
