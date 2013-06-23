ModuleGroupFactory = require '../module_group_factory'

# This convention attempts to adhere to a sane and relatively "standard" set of principles that
# should work for most node modules.
#
# Of course, there really isn't anything close to a standard naming & package layout style, so
# you might want to take a look at the other conventions if the defaults don't do it for you.
# Or write your own!
#
# Any options not specified in any other conventions fall back to those defined in this one.

# ## An Example
#
# Given this project hierarchy:
#
#     lib/
#       things-and-baubles/
#         stuff/
#           moo.js
#         gizmo.js
#         many-doohickey.js
#       fizz.js
#       fizz_bam.js
#
# The exported module hierarchy will be as follows (assuming that lib/ is autorequired):
#
#     {
#       thingsAndBaubles: {
#         stuff: {
#           moo: <module.exports from moo.js>
#         },
#         gizmo: <module.exports from gizmo.js>
#         manyDoohickey: <module.exports from many-doohickey.js>
#       },
#       fizz: <module.exports from fizz.js>
#       fizzBam: <module.exports from fizz_bam.js>
#     }

class Default
  # ## Core Functionality

  # Given a directory, build and return the root module group for it.
  buildRootModuleGroup: (rootPath) ->
    ModuleGroupFactory.buildModuleGroup @, rootPath

  # ## Naming Conventions

  # You can special case specific module names by defining them keyed on the file name without an
  # extension.  For example: {cli: 'CLI'} would force cli.js to be named CLI.
  specialCaseModuleNames: {}

  # Given a directory name and parent path, return the property name that should be used to
  # represent it in the module's hierarchy.
  #
  # The default is to convert to a `camelCase` style (splitting on - and _).
  directoryToProperty: (directoryName, parentPath) ->
    @specialCaseModuleNames[directoryName] or @camelCase directoryName

  # Given a file name and parent path, return the property that should be used to represent it in
  # the module's hierarchy.
  #
  # The default is to expose the file's exports as a `camelCase` form of the file name.
  fileToProperty: (fileName, parentPath) ->
    baseName = @stripFileExtension fileName

    @specialCaseModuleNames[baseName] or @camelCase baseName

  # ## Autorequired Source Evaluation

  # Gives the convention an opportunity to modify the sandbox of an autorequired source file before
  # it is loaded.  Returns the sandbox that was modified (in case you want to replace it).
  #
  # By default, this registers several sets of globals for the module.  See [`globalLazyLoads`]
  # (#section-13).
  modifySandbox: (sandbox, module) ->
    module._globalLazyLoads = @globalLazyLoads(module)
    module._require         = sandbox.require

    sandbox

  # Gives the convention an opportunity to modify the source of a file before it is loaded.
  # Returns the modified source.
  #
  # By default, this works in concert with [`globalLazyLoads`](#section-13) and sets up the
  # registered properties to be lazy loaded.  We have to drop down to the source level because
  # the module is evaluated in a sandboxed context.  The global context is shallow copied when
  # entering or leaving the sandbox, and getter/setter properties are not preserved.
  modifySource: (source, module) ->
    """
    for (var key in module._globalLazyLoads) {
      try {
        Object.defineProperty(global, key, {
          enumerable: false, configurable: true, get: module._globalLazyLoads[key]
        });
      } catch (err) {}
    }
    """ + source

  # Gives the convention an opportunity to modify the exports before they are returned.  The
  # convention can also peek into the state of the sandbox after evaluation if it wishes to pull out
  # any properties defined globally within the evaluated module.  Returns the modified exports.
  #
  # By default, this is a no-op.
  modifyExports: (exports, module) ->
    exports

  # ## Autorequired Module Globals

  # Modules to globally register for autorequiring.  All the core node libraries.
  #
  # Each one will be registered with a name adhering to [`directoryToProperty`](#section-4).
  # Notably absent is module, as it defines a prototype (and is not a namespaced module).
  #
  # You probably don't want to override this list.  If you wish to add extra modules, define them
  # via [`extraGlobalAutorequires`](#section-12)
  globalModules: [
    'assert', 'buffer', 'child_process', 'constants', 'crypto', 'dgram', 'dns', 'events',
    'freelist', 'fs', 'http', 'https', 'net', 'os', 'path', 'querystring', 'readline', 'repl',
    'stream', 'string_decoder', 'sys', 'timers', 'tls', 'tty', 'url', 'util', 'vm'
  ]

  # Any additional modules to add to the [`globalLazyLoads`](#section-13) without modifying that
  # list.
  extraGlobalModules: []

  # Returns an object of properties that should be lazy loaded as global objects in an autorequired
  # source file.
  #
  # It should be a map of property names to functions (that will be called on first reference, the
  # result is then memoized).
  #
  # This registers several sets of globals for the module:
  globalLazyLoads: (module) ->
    result = {}

    @appendGlobalModules(result, module)
    @appendProjectModules(result, module)

    result

  # * All globals defined in `appendGlobalModules` are registered to be autorequired.
  appendGlobalModules: (lazyLoads, module) ->
    for mod in @globalModules.concat @extraGlobalModules
      do (mod) =>
        lazyLoads[@directoryToProperty(mod)] = -> module._require mod

  # * All autorequireable properties visible to the current directory and above.
  appendProjectModules: (lazyLoads, module) ->
    moduleGroup = module.autorequireParent

    while moduleGroup
      for key of moduleGroup
        # Don't allow ambiguous properties
        throw new Error "Ambiguous property '#{key}'" if key of lazyLoads # TODO: Better error
        # And don't proxy the current module
        continue if key == module.id

        do (key, moduleGroup) -> lazyLoads[key] = -> moduleGroup[key]

      moduleGroup = moduleGroup.__parent

  # For example, from within `lib/things/gizmo.js` (as part of the example outlined above):
  #
  # * You can reference any of the core node libraries without requiring them.
  # * `ManyDoohickey` and `stuff` would be available for referencing.
  # * `things` would also be available in order to traverse up the hierarchy.
  #
  # Every one of these properties is lazy-loaded.

  # ## Helpers & Utility

  # Strips the extension from a file name.
  stripFileExtension: (fileName) ->
    fileName.match(/(.+?)(\.[^.]*$|$)/)[1]

  # CamelCaps a path component.
  camelCaps: (pathComponent) ->
    pathComponent.split(/[-_]+/).map((val) -> val[0].toLocaleUpperCase() + val[1..]).join ''

  # camelCase a path component.
  camelCase: (pathComponent) ->
    result = @camelCaps pathComponent
    result[0].toLocaleLowerCase() + result[1..]

  # under_score a path component.
  underscore: (pathComponent) ->
    pathComponent.split(/[-_]+/).join '_'

module.exports = Default
