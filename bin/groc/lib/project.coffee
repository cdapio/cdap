# # groc API

# A core concept of `groc` is that your code is grouped into a project, and that there is a certain
# amount of context that it lends to your documentation.
#
# A project:
class Project
  constructor: (root, outPath, minLogLevel=utils.Logger::INFO) ->
    @log = new utils.Logger minLogLevel

    # * Has a single root directory that contains (most of) it.
    @root = path.resolve root
    # * Generally wants documented generated somewhere within its tree.  We default the output path
    #   to be relative to the project root, unless you pass an absolute path.
    @outPath = path.resolve @root, outPath
    # * Contains a set of files to generate documentation from, source code or otherwise.
    @files = []
    # * Should strip specific prefixes of a file's path when generating relative paths for
    #   documentation.  For example, this could be used to ensure that `lib/some/source.file` maps
    #   to `doc/some/source.file` and not `doc/lib/some/source.file`.
    @stripPrefixes = []

  # This is both a performance (over-)optimization and debugging aid.  Instead of spamming the
  # system with file I/O and overhead all at once, we only process a certain number of source files
  # concurrently.  This is similar to what [graceful-fs](https://github.com/isaacs/node-graceful-fs)
  # accomplishes.
  BATCH_SIZE: 10

  # Where the magic happens.
  #
  # Currently, the only supported option is:
  generate: (options, callback) ->
    @log.trace 'Project#Generate(%j, ...)', options
    @log.info 'Generating documentation...'

    # * style: The style prototype to use.  Defaults to `styles.Default`
    style = new (options.style || styles.Default) @

    # We need to ensure that the project root is a strip prefix so that we properly generate
    # relative paths for our files.  Since strip prefixes are relative, it must be the first prefix,
    # so that they can strip from the remainder.
    @stripPrefixes = ["#{@root}/"].concat @stripPrefixes

    fileMap   = Utils.mapFiles @root, @files, @stripPrefixes
    indexPath = path.resolve @root, @index

    pool = spate.pool (k for k of fileMap), maxConcurrency: @BATCH_SIZE, (currentFile, done) =>
      @log.debug "Processing %s", currentFile

      language = Utils.getLanguage currentFile
      unless language?
        @log.warn '%s is not in a supported language, skipping.', currentFile
        return done()

      fs.readFile currentFile, 'utf-8', (error, data) =>
        if error
          @log.error "Failed to process %s: %s", currentFile, error.message
          return callback error

        fileInfo =
          language:    language
          sourcePath:  currentFile
          projectPath: currentFile.replace ///^#{@root + '/'}///, ''
          targetPath:  if currentFile == indexPath then 'index' else fileMap[currentFile]

        style.renderFile data, fileInfo, done

    pool.exec (error) =>
      return callback error if error

      style.renderCompleted (error) =>
        return callback error if error

        @log.info ''
        @log.pass 'Documentation generated'
        callback()
