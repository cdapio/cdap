class Base
  constructor: (project) ->
    @project = project
    @log     = project.log
    @files   = []
    @outline = {} # Keyed on target path

  renderFile: (data, fileInfo, callback) ->
    @log.trace 'BaseStyle#renderFile(..., %j, ...)', fileInfo

    @files.push fileInfo

    segments = Utils.splitSource data, fileInfo.language

    @log.debug 'Split %s into %d segments', fileInfo.sourcePath, segments.length

    Utils.highlightCode segments, fileInfo.language, (error) =>
      if error
        if error.failedHighlights
          for highlight, i in error.failedHighlights
            @log.debug "highlight #{i}:"
            @log.warn   segments[i]?.code.join '\n'
            @log.error  highlight

        @log.error 'Failed to highlight %s as %s: %s', fileInfo.sourcePath, fileInfo.language.name, error.message or error
        return callback error

      Utils.markdownComments segments, @project, (error) =>
        if error
          @log.error 'Failed to markdown %s: %s', fileInfo.sourcePath, error.message
          return callback error

        @outline[fileInfo.targetPath] = utils.StyleHelpers.outlineHeaders segments

        # We also prefer to split out solo headers
        segments = utils.StyleHelpers.segmentizeSoloHeaders segments

        @renderDocFile segments, fileInfo, callback

  renderDocFile: (segments, fileInfo, callback) ->
    @log.trace 'BaseStyle#renderDocFile(..., %j, ...)', fileInfo

    throw new Error "@templateFunc must be defined by subclasses!" unless @templateFunc

    docPath = path.resolve @project.outPath, "#{fileInfo.targetPath}.html"

    fsTools.mkdir path.dirname(docPath), 0755, (error) =>
      if error
        @log.error 'Unable to create directory %s: %s', path.dirname(docPath), error.message
        return callback error

      for segment in segments
        segment.markdownedComments = Utils.trimBlankLines segment.markdownedComments
        segment.highlightedCode    = Utils.trimBlankLines segment.highlightedCode

      templateContext =
        project:     @project
        segments:    segments
        sourcePath:  fileInfo.sourcePath
        targetPath:  fileInfo.targetPath
        projectPath: fileInfo.projectPath

      # How many levels deep are we?
      pathChunks = path.dirname(fileInfo.targetPath).split(/[\/\\]/)
      if pathChunks.length == 1 && pathChunks[0] == '.'
        templateContext.relativeRoot = ''
      else
        templateContext.relativeRoot = "#{pathChunks.map(-> '..').join '/'}/"

      try
        data = @templateFunc templateContext

      catch error
        @log.error 'Rendering documentation template for %s failed: %s', docPath, error.message
        return callback error

      fs.writeFile docPath, data, 'utf-8', (error) =>
        if error
          @log.error 'Failed to write documentation file %s: %s', docPath, error.message
          return callback error

        @log.pass docPath
        callback()

  renderCompleted: (callback) ->
    @log.trace 'BaseStyle#renderCompleted(...)'

    @tableOfContents = utils.StyleHelpers.buildTableOfContents @files, @outline

    callback()
