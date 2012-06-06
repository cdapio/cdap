tableOfContents = <%= JSON.stringify(tableOfContents) %>

# # Page Behavior

# ## Table of Contents

# Global jQuery references to navigation components we care about.
nav$ = null
toc$ = null

setTableOfContentsActive = (active) ->
  html$ = $('html')

  if active
    nav$.addClass  'active'
    html$.addClass 'popped'
  else
    nav$.removeClass  'active'
    html$.removeClass 'popped'

toggleTableOfContents = ->
  setTableOfContentsActive not nav$.hasClass 'active'

# ### Node Navigation
currentNode$ = null

focusCurrentNode = ->
  # We use the first child's offset top rather than toc$.offset().top because there may be borders
  # or other stylistic tweaks that further offset the scrollTop.
  currentNodeTop    = currentNode$.offset().top - toc$.children(':visible').first().offset().top
  currentNodeBottom = currentNodeTop + currentNode$.children('.label').height()

  # If the current node is partially or fully above the top of the viewport, scroll it into view.
  if currentNodeTop < toc$.scrollTop()
    toc$.scrollTop currentNodeTop

  # Similarly, if we're below the bottom of the viewport, scroll up enough to make it visible.
  if currentNodeBottom > toc$.scrollTop() + toc$.height()
    toc$.scrollTop currentNodeBottom - toc$.height()

setCurrentNodeExpanded = (expanded) ->
  if expanded
    currentNode$.addClass 'expanded'
  else
    if currentNode$.hasClass 'expanded'
      currentNode$.removeClass 'expanded'

    # We collapse up to the node's parent if the current node is already collapsed.  This allows
    # a user to quickly spam left to move up the tree.
    else
      parents$ = currentNode$.parents('li')
      selectNode parents$.first() if parents$.length > 0

  focusCurrentNode()

selectNode = (newNode$) ->
  # Remove first, in case it's the same node
  currentNode$.removeClass 'selected'
  newNode$.addClass 'selected'

  currentNode$ = newNode$
  focusCurrentNode()

selectNodeByDocumentPath = (documentPath, headerSlug=null) ->
  currentNode$ = fileMap[documentPath]
  if headerSlug
    for link in currentNode$.find '.outline a'
      urlChunks = $(link).attr('href').split '#'

      if urlChunks[1] == headerSlug
        currentNode$ = $(link).parents('li').first()
        break

  currentNode$.addClass 'selected expanded'
  currentNode$.parents('li').addClass 'expanded'

  focusCurrentNode()

moveCurrentNode = (up) ->
  visibleNodes$ = toc$.find 'li:visible:not(.filtered)'

  # Fall back to the first node if anything goes wrong
  newIndex = 0
  for node, i in visibleNodes$
    if node == currentNode$[0]
      newIndex = if up then i - 1 else i + 1
      newIndex = 0 if newIndex < 0
      newIndex = visibleNodes$.length - 1 if newIndex > visibleNodes$.length - 1
      break

  selectNode $(visibleNodes$[newIndex])

visitCurrentNode = ->
  labelLink$ = currentNode$.children('a.label')
  window.location = labelLink$.attr 'href' if labelLink$.length > 0


# ## Node Search

# Only show a filter if it matches this many or fewer nodes
MAX_FILTER_SIZE = 10

# An array of of [search string, node, label text] triples
searchableNodes = []
appendSearchNode = (node$) ->
  text$ = node$.find('> .label .text')
  searchableNodes.push [text$.text().toLowerCase(), node$, text$]

currentQuery = ''
searchNodes = (queryString) ->
  queryString = queryString.toLowerCase().replace(/\s+/, '')
  return if queryString == currentQuery
  currentQuery = queryString

  return clearFilter() if queryString == ''

  matcher  = new RegExp (c.replace /[-[\]{}()*+?.,\\^$|#\s]/, "\\$&" for c in queryString).join '.*'
  matched  = []
  filtered = []

  for nodeInfo in searchableNodes
    if matcher.test nodeInfo[0] then matched.push nodeInfo else filtered.push nodeInfo

  return clearFilter() if matched.length > MAX_FILTER_SIZE

  nav$.addClass 'searching'

  # Update the DOM
  for nodeInfo in filtered
    nodeInfo[1].removeClass 'matched-child'
    nodeInfo[1].addClass 'filtered'
    clearHighlight nodeInfo[2]

  for nodeInfo in matched
    nodeInfo[1].removeClass 'filtered matched-child'
    nodeInfo[1].addClass 'matched'

    highlightMatch nodeInfo[2], queryString

    # Filter out our immediate parent
    $(p).addClass 'matched-child' for p in nodeInfo[1].parents 'li'

clearFilter = ->
  nav$.removeClass 'searching'
  currentQuery = ''

  for nodeInfo in searchableNodes
    nodeInfo[1].removeClass 'filtered matched-child'
    clearHighlight nodeInfo[2]

highlightMatch = (text$, queryString) ->
  nodeText  = text$.text()
  lowerText = nodeText.toLowerCase()

  markedText    = ''
  furthestIndex = 0

  for char in queryString
    foundIndex    = lowerText.indexOf char, furthestIndex
    markedText   += nodeText[furthestIndex...foundIndex] + "<em>#{nodeText[foundIndex]}</em>"
    furthestIndex = foundIndex + 1

  text$.html markedText + nodeText[furthestIndex...]

clearHighlight = (text$) ->
  text$.text text$.text() # Strip all tags


# ## DOM Construction
#
# Navigation and the table of contents are entirely managed by us.
fileMap = {} # A map of targetPath -> DOM node

buildNav = (metaInfo) ->
  nav$ = $("""
    <nav>
      <ul class="tools">
        <li class="toggle">Table of Contents</li>
        <li class="search">
          <input id="search" type="search" autocomplete="off"/>
        </li>
      </ul>
      <ol class="toc"/>
      </div>
    </nav>
  """).appendTo $('body')
  toc$ = nav$.find '.toc'

  if metaInfo.githubURL
    # Special case the index to go to the project root
    if metaInfo.documentPath == 'index'
      sourceURL = metaInfo.githubURL
    else
      sourceURL = "#{metaInfo.githubURL}/blob/master/#{metaInfo.projectPath}"

    nav$.find('.tools').prepend """
      <li class="github">
        <a href="#{sourceURL}" title="View source on GitHub">
          View source on GitHub
        </a>
      </li>
    """

  for node in tableOfContents
    toc$.append buildTOCNode node, metaInfo

  nav$

buildTOCNode = (node, metaInfo) ->
  node$ = $("""<li class="#{node.type}"/>""")

  switch node.type
    when 'file'
      #} Single line to avoid extra whitespace
      node$.append """<a class="label" href="#{metaInfo.relativeRoot}#{node.data.targetPath}.html" title="#{node.data.projectPath}"><span class="text">#{node.data.title}</span></a>"""

    when 'folder'
      node$.append """<span class="label"><span class="text">#{node.data.title}</span></span>"""

  if node.children?.length > 0
    children$ = $('<ol class="children"/>')
    children$.append buildTOCNode c, metaInfo for c in node.children

    node$.append children$

  label$ = node$.find('> .label')
  label$.click -> selectNode node$

  discloser$ = $('<span class="discloser"/>').prependTo label$
  discloser$.addClass 'placeholder' unless node.children?.length > 0
  discloser$.click (evt) ->
    node$.toggleClass 'expanded'
    evt.preventDefault()

  # Persist our references to the node
  fileMap[node.data.targetPath] = node$ if node.type == 'file'
  appendSearchNode node$

  node$

$ ->
  metaInfo =
    relativeRoot: $('meta[name="groc-relative-root"]').attr('content')
    githubURL:    $('meta[name="groc-github-url"]').attr('content')
    documentPath: $('meta[name="groc-document-path"]').attr('content')
    projectPath:  $('meta[name="groc-project-path"]').attr('content')

  nav$    = buildNav metaInfo
  toc$    = nav$.find '.toc'
  search$ = $('#search')

  # Select the current file, and expand up to it
  selectNodeByDocumentPath metaInfo.documentPath, window.location.hash.replace '#', ''

  # We use the search box's focus state to toggle the table of contents.  This ensures that search
  # will always be focused while the toc is up, and that it goes away once the user clicks off.
  search$.focus -> setTableOfContentsActive true

  # However, we don't want to hide the table of contents if you are clicking around in the nav.
  #
  # The blur event doesn't give us the previous event, sadly, so we first trap mousedown events
  lastMousedownTimestamp = null
  nav$.mousedown (evt) ->
    lastMousedownTimestamp = evt.timeStamp unless evt.target == toggle$[0]

  # And we refocus search if we are within a very short duration between the last mousedown in nav$.
  search$.blur (evt) ->
    if evt.timeStamp - lastMousedownTimestamp < 10
      search$.focus()
    else
      setTableOfContentsActive false

  # Set up the table of contents toggle
  toggle$ = nav$.find '.toggle'
  toggle$.click (evt) ->
    if search$.is ':focus' then search$.blur() else search$.focus()
    evt.preventDefault()

  # Prevent text selection if the user taps quickly
  toggle$.mousedown (evt) ->
    evt.preventDefault()

  # Arrow keys navigate the table of contents whenever it is visible
  $('body').keydown (evt) ->
    if nav$.hasClass 'active'
      switch evt.keyCode
        when 13 then visitCurrentNode() # return
        when 37 then setCurrentNodeExpanded false # left
        when 38 then moveCurrentNode true # up
        when 39 then setCurrentNodeExpanded true # right
        when 40 then moveCurrentNode false # down
        else return

      evt.preventDefault()

  # searching
  search$.bind 'keyup search', (evt) ->
    searchNodes search$.val()

  search$.keydown (evt) ->
    if evt.keyCode == 27 # Esc
      if search$.val().trim() == ''
        search$.blur()
      else
        search$.val ''
