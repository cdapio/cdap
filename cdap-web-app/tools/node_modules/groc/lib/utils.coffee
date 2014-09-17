# Miscellaneous code fragments reside here.
Utils =
  # Escape regular expression characters in a string
  #
  # Code from http://zetafleet.com/ via http://simonwillison.net/2006/Jan/20/escape/
  regexpEscape: (string) ->
    string.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, '\\$&')

  # Detect and return the language that a given file is written in.
  #
  # The language is also annotated with a name property, matching the laguages key in LANGUAGES.
  getLanguage: (filePath) ->
    unless @_languageDetectionCache?
      @_languageDetectionCache = []

      for name, language of LANGUAGES
        language.name = name

        for matcher in language.nameMatchers
          # If the matcher is a string, we assume that it's a file extension.  Stick it in a regex:
          matcher = ///#{@regexpEscape matcher}$/// if _.isString matcher

          @_languageDetectionCache.push [matcher, language]

    baseName = path.basename filePath

    for pair in @_languageDetectionCache
      return pair[1] if baseName.match pair[0]

  # Map a list of file paths to relative target paths by stripping prefixes off of them.
  mapFiles: (resolveRoot, files, stripPrefixes) ->
    # Ensure that we're dealing with absolute paths across the board
    files = files.map (f) -> path.resolve resolveRoot, f
    # And that the strip prefixes all end with a /, to avoid a target path being absolute.
    stripPrefixes = stripPrefixes.map (p) -> path.join( "#{path.resolve resolveRoot, p}/" )

    # Prefixes are stripped in order of most specific to least (# of directories deep)
    prefixes = stripPrefixes.sort (a,b) => @pathDepth(b) - @pathDepth(a)

    result = {}

    for absPath in files
      file = absPath

      for stripPath in stripPrefixes
        file = file[stripPath.length..] if file[0...stripPath.length] == stripPath

      # We also strip the extension under the assumption that the consumer of this path map is going
      # to substitute in their own.  Plus, if they care about the extension, they can get it from
      # the keys of the map.
      result[absPath] = file[0...-path.extname(file).length]

    result

  # Attempt to guess strip prefixes for a given set of arguments.
  guessStripPrefixes: (arguments) ->
    result = []
    for arg in arguments
      # Most globs look something like dir/**/*.ext, so strip up to the leading *
      arg = arg.replace /\*.*$/, ''

      result.push arg if arg.slice(-1) == '/'

    # For now, we try to avoid ambiguous situations by guessing the FIRST directory given.  The
    # assumption is that you don't want merged paths, but probably did specify the most important
    # source directory first.
    result = _(result).uniq()[...1]

  # How many directories deep is a given path?
  pathDepth: (path) ->
    path.split(/[\/\\]/).length

  # Split source code into segments (comment + code pairs)
  splitSource: (data, language) ->
    lines = data.split /\r?\n/

    # Always strip shebangs - but don't shift it off the array to avoid the perf hit of walking the
    # array to update indices.
    lines[0] = '' if lines[0][0..1] == '#!'

    # Special case: If the language is comments-only, we can skip pygments
    return [new @Segment [], lines] if language.commentsOnly

    segments = []
    currSegment = new @Segment

    # We only support single line comments for the time being.
    singleLineMatcher = ///^\s*(#{language.singleLineComment.join('|')})\s?(.*)$///

    for line in lines
      # Match that line to the language's single line comment syntax.
      #
      # However, we treat all comments beginning with } as inline code commentary.
      match = line.match singleLineMatcher

      #} For example, this comment should be treated as part of our code.
      if match? and match[2]?[0] != '}'
        if currSegment.code.length > 0
          segments.push currSegment
          currSegment = new @Segment

        currSegment.comments.push match[2]

      else
        currSegment.code.push line

    segments.push currSegment
    segments

  # Just a convenient prototype for building segments
  Segment: class Segment
    constructor: (code=[], comments=[]) ->
      @code     = code
      @comments = comments

  # Annotate an array of segments by running their code through [Pygments](http://pygments.org/).
  highlightCode: (segments, language, callback) ->
    # Don't bother spawning pygments if we have nothing to highlight
    numCodeLines = segments.reduce ( (c,s) -> c + s.code.length ), 0
    if numCodeLines == 0
      for segment in segments
        segment.highlightedCode = ''

      return callback()

    pygmentize = childProcess.spawn 'pygmentize', [
      '-l', language.pygmentsLexer
      '-f', 'html'
      '-O', 'encoding=utf-8,tabsize=2'
    ]
    pygmentize.stderr.addListener 'data', (data)  -> callback data.toString()
    pygmentize.stdin.addListener 'error', (error) ->
      # This appears to only occur when pygmentize is missing:
      utils.Logger.error "Unable to find 'pygmentize' on your PATH.  Please install pygments."
      utils.Logger.info ''

      # Lack of pygments is a one time setup task, we don't feel bad about killing the process
      # off until the user does so.  It's a hard requirement.
      process.exit 1

    # We'll just split the output at the end.  pygmentize doesn't stream its output, and a given
    # source file is small enough that it shouldn't matter.
    result = ''
    pygmentize.stdout.addListener 'data', (data) =>
      result += data.toString()

    pygmentize.addListener 'exit', (args...) =>
      # pygments spits it out wrapped in `<div class="highlight"><pre>...</pre></div>`.  We want to
      # manage the styling ourselves, so remove that.
      result = result.replace('<div class="highlight"><pre>', '').replace('</pre></div>', '')

      # Extract our segments from the pygmentized source.
      highlighted = "\n#{result}\n".split /.*<span.*SEGMENT DIVIDER<\/span>.*/

      if highlighted.length != segments.length
        error = new Error utils.CompatibilityHelpers.format 'pygmentize rendered %d of %d segments; expected to be equal',
          highlighted.length, segments.length

        error.pygmentsOutput   = result
        error.failedHighlights = highlighted
        return callback error

      # Attach highlighted source to the highlightedCode property of a Segment.
      for segment, i in segments
        segment.highlightedCode = highlighted[i]

      callback()

    # Rather than spawning pygments for each segment, we stream it all in, separated by 'magic'
    # comments so that we can split the highlighted source back into segments.
    #
    # To further complicate things, pygments doesn't let us cheat with indentation-aware languages:
    # We have to match the indentation of the line following the divider comment.
    mergedCode = ''
    for segment, i in segments
      segmentCode = segment.code.join '\n'

      if i > 0
        # Double negative: match characters that are spaces but not newlines
        indentation = segmentCode.match(/^[^\S\n]+/)?[0] || ''
        mergedCode += "\n#{indentation}#{language.singleLineComment[0]} SEGMENT DIVIDER\n"

      mergedCode += segmentCode

    pygmentize.stdin.write mergedCode
    pygmentize.stdin.end()

  # Annotate an array of segments by running their comments through
  # [showdown](https://github.com/coreyti/showdown).
  markdownComments: (segments, project, callback) ->
    converter = new showdown.Showdown.converter()

    try
      for segment, segmentIndex in segments
        markdown = converter.makeHtml segment.comments.join '\n'
        headers  = []

        # showdown generates header ids by lowercasing & dropping non-word characters.  We'd like
        # something a bit more readable.
        markdown = @gsub markdown, /<h(\d) id="[^"]+">([^<]+)<\/h\d>/g, (match) =>
          header =
            level: parseInt match[1]
            title: match[2]
            slug:  @slugifyTitle match[2]

          header.isFileHeader = true if header.level == 1 && segmentIndex == 0 && match.index == 0

          headers.push header

          "<h#{header.level} id=\"#{header.slug}\">#{header.title}</h#{header.level}>"

        # We attach the rendered markdown to the comment
        segment.markdownedComments = markdown
        # As well as the extracted headers to aid in outline building.
        segment.headers = headers

    catch error
      return callback error

    callback()

  # Sometimes you just don't want any of them hanging around.
  trimBlankLines: (string) ->
    string.replace(/^[\r\n]+/, '').replace(/[\r\n]+$/, '')

  # Given a title, convert it into a URL-friendly slug.
  slugifyTitle: (string) ->
    string.split(/[\s\-\_]+/).map( (s) -> s.replace(/[^\w]/g, '').toLowerCase() ).join '-'

  # replacer is a function that is given the match object, and returns the string to replace with.
  gsub: (string, matcher, replacer) ->
    throw new Error 'You must pass a global RegExp to gsub!' unless matcher.global?

    result = ''
    matcher.lastIndex = 0
    furthestIndex = 0

    while (match = matcher.exec string) != null
      result += string[furthestIndex...match.index] + replacer match

      furthestIndex = matcher.lastIndex

    result + string[furthestIndex...]
