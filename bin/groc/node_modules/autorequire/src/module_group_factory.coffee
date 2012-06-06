fs   = require 'fs'
path = require 'path'

utils  = require './utils'
Loader = require './loader'


# A factory that generates module groups.
#
# In order to keep a module group's properties lean and mean, going with a classical approach is
# overkill, and we don't want to have potential naming conflicts.
class ModuleGroupFactory
  # Each module group gets an instance of the factory, which is effectively the behavior and private
  # state of the module group.
  @buildModuleGroup: (args...) ->
    newFactory = new @(args...)
    newFactory.build()

  # Set up a factory instance to build the module group.
  constructor: (convention, path, name='__root', parent) ->
    # * The convention to use when determining the name of modules (files) and sub-groups
    #   (directories) within the module group.
    @convention = convention
    # * The _absolute_ file system path of the directory to walk when building the module group.
    @path = path
    # * The symbolic name of the module group.
    @name = name
    # * The parent module group to this one, or null.
    @parent = parent

  # Builds and returns the module group
  build: ->
    @moduleGroup = {}

    @appendIntrospectiveProperties()
    @enumerateModuleDirectory()

    @moduleGroup

  # Sticks useful non-enumerable properties on the module group:
  appendIntrospectiveProperties: ->
    # * The file system path to where this module group is defined.
    Object.defineProperty @moduleGroup, '__dirname', value: @path, enumerable: false
    # * The module group's named path.
    Object.defineProperty @moduleGroup, '__name', value: @name, enumerable: false
    # * The module group's parent group.
    Object.defineProperty @moduleGroup, '__parent', value: @parent, enumerable: false

  # Enumerate the module group's directory and define its properties
  enumerateModuleDirectory: ->
    for pathComponent in fs.readdirSync @path when pathComponent[0] != '.'
      fullPath  = path.join(@path, pathComponent)
      pathStats = fs.statSync(fullPath)

      do (fullPath) =>
        # Directories recurse (upon lazy-load).
        if pathStats.isDirectory()
          groupName = @convention.directoryToProperty pathComponent, @path
          utils.lazyLoad @moduleGroup, groupName, => @buildSubGroup groupName, fullPath

        # Files are considered to be components, they're rquired upon lazy-load.
        else if pathStats.isFile()
          componentName = @convention.fileToProperty pathComponent, @path
          utils.lazyLoad @moduleGroup, componentName, => @loadModule componentName, fullPath

  # Builds a child module group for a subdirectory
  buildSubGroup: (groupName, path) ->
    @constructor.buildModuleGroup @convention, path, "#{@name}.#{groupName}", @moduleGroup

  # Loads and returns a component module
  loadModule: (componentName, path) ->
    Loader.loadModule componentName, path, @moduleGroup, @convention

module.exports = ModuleGroupFactory
