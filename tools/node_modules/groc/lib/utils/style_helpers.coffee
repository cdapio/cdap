# # Style Helpers
#
# A collection of helpful functions to support styles and their behavior.
StyleHelpers =
  # Generate a table of contents as a tree of {node: {...}, children: []} objects.
  #
  # We want a pretty complex hierarchy in our table of contents:
  # buildTableOfContents: (files) ->

  # Given an array of markdowned segments, convert their list of headers into a hierarchical
  # outline (table of contents!)
  outlineHeaders: (segments) ->
    headers = segments.reduce ( (a, s) -> a.concat s.headers ), []
    return [] unless headers.length > 0

    nodes = []
    for header in headers
      nodes.push
        type: 'heading'
        data:  header
        depth: header.level

    @buildNodeTree nodes

  # Generate a table of contents as a tree of {..., children: []} objects.
  #
  # We take a list of file info objects, and a map of outlines to fileInfo.targetPath.
  #
  # We want a pretty complex hierarchy in our table of contents:
  buildTableOfContents: (files, outlines) ->
    files = files.sort (a, b) ->
      # * The index is always first in the table of contents.
      return -1 if a.targetPath == 'index'
      return  1 if b.targetPath == 'index'

      return 0 if a.targetPath == b.targetPath
      # * files matching a directory name come directly before it.  E.g. "foo" < "foo/bar" < "foz"
      if a.targetPath < b.targetPath then -1 else 1

    nodes    = []
    prevPath = []
    for file in files
      targetChunks = file.targetPath.split path.join('/')
      pathChunks   = targetChunks[0...-1]

      # * If a file has the same name as a directory, it takes ownership of that directory's folder.
      for chunk, i in pathChunks
        if prevPath[i] != chunk
          # * Otherwise we have a generic folder node that takes its place in the table of contents.
          #   TODO: We probably want some way to rename folders!
          nodes.push
            type:  'folder'
            data:
              path:  targetChunks[0..i].join '/'
              title: targetChunks[i]
            depth: i + 1

          prevPath = [] # Make sure that we don't match directories several levels in, after a fail.

      prevPath = targetChunks

      fileData = _(file).clone()

      # Annotate the file data with a title to represent it.
      #
      # If possible, we use the initial header in the file,
      if outlines[file.targetPath]?[0]?.data?.isFileHeader
        fileData.firstHeader = outlines[file.targetPath].shift()
        fileData.title       = fileData.firstHeader.data.title

        if fileData.firstHeader.children?.length > 0
          outlines[file.targetPath] = fileData.firstHeader.children.concat outlines[file.targetPath]

      # Otherwise we just fall back to the file's target path...
      else
        fileData.title = path.basename file.targetPath

      nodes.push
        type:   'file'
        data:    fileData
        depth:   file.targetPath.split( path.join('/') ).length
        outline: outlines[file.targetPath]

    @buildNodeTree nodes

  # Take a flat, though ordered, list of nodes and convert them into a tree.
  #
  # Nodes are expected to have a `depth` property.  Each node is annotated with an array of
  # `children` if it does not exist.  Children are appended, otherwise.
  buildNodeTree: (nodes) ->
    result = []
    stack  = []

    for node in nodes
      # Unwind the stack until we get to the first node that is at a lower depth than us.  We are
      # considered to be its child
      stack.pop() while _(stack).last()?.depth >= node.depth

      if stack.length == 0
        # Top level nodes are directly returned in the result list.
        result.push node
      else
        _(stack).last().children ||= []
        _(stack).last().children.push node

      stack.push node

    result

  # It helps visually to separate headers that are not attached to other comments into their own
  # segment.
  segmentizeSoloHeaders: (segments) ->
    results = []
    for segment in segments
      headerOnly = segment.markdownedComments.match /^\s*<h\d[^>]*>[^<]*<\/h\d>\s*$/
      if headerOnly and not segment.highlightedCode.match /^\s*$/
        results.push
          code:               []
          comments:           segment.comments
          highlightedCode:    ''
          markdownedComments: segment.markdownedComments

        results.push
          code:               segment.code
          comments:           []
          highlightedCode:    segment.highlightedCode
          markdownedComments: ''

      else
        results.push segment

    results
