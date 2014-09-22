fs   = require 'fs'
path = require 'path'

utils  = require './utils'
Loader = require './loader'
ModuleGroupFactory = require './module_group_factory'

# ## autorequire()
#
# Set up an autorequired module at the specified path.  The path must be a relative file system
# path.
autorequire = (requirePath, conventionAndOrOverrides...) ->
  raise TypeError, 'autorequire only supports ./relative paths for now.' unless requirePath[0] == '.'

  workingDir = utils.getCallingDirectoryFromStack()
  rootPath   = path.normalize workingDir + '/' + requirePath

  # You can optionally specify a convention, as well as overrides to individual methods in that
  # convention:
  overrides = conventionAndOrOverrides.pop() if typeof conventionAndOrOverrides[conventionAndOrOverrides.length - 1] == 'object'

  # * autorequire defaults to [`conventions.Default`](conventions/default.html)
  convention = conventionAndOrOverrides.shift() or 'Default'

  # * Conventions that are bundled with the autorequire package can be specified by string; they are
  #   loaded by looking up the class by the same name underneath `autorequire.conventions`.
  if typeof convention == 'string'
    throw new TypeError "There is no built-in '#{convention}' convention" unless conventions[convention]
    conventionPrototype = conventions[convention]

  # * Otherwise, you can specify a convention by passing its constructor function.
  if typeof convention == 'function'
    conventionPrototype = convention

  # If you pass an options hash, a custom convention will be built by inheriting the convention you
  # specified (or the Default convention), setting each propery in the hash to the new convention's
  # prototype.
  #
  # For a full reference of the methods available to a convention, take a look at
  # [`conventions.Default`](conventions/default.html).
  if overrides
    class CustomConvention extends conventionPrototype
    for own key, value of overrides
      CustomConvention::[key] = value

    conventionPrototype = CustomConvention

  unless conventionPrototype?
    throw new TypeError 'autorequire was unable to determine a valid convention, please check your arguments.'

  convention = new conventionPrototype
  convention.buildRootModuleGroup rootPath


# ## Public API Properties

# Set up all our conventions to be lazy-loaded so that they can be inherited from with minimal
# performance hit.
conventions = {}
for file in fs.readdirSync path.join(__dirname, 'conventions') when file != '.'
  convention = path.basename(file, path.extname(file))
  do (convention) ->
    conventionName = convention.split(/[-_]+/).map((val) -> val[0].toLocaleUpperCase() + val[1..]).join ''
    conventionName = conventionName[0].toLocaleUpperCase() + conventionName[1..]

    utils.lazyLoad conventions, conventionName, ->
      require "./conventions/#{convention}"

# Exported properties; they adhere to the [`Default`](conventions/default.html) convention.
module.exports = autorequire
autorequire.conventions = conventions
autorequire.Loader = Loader
autorequire.ModuleGroupFactory = ModuleGroupFactory
