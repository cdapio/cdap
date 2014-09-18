# Not classical in the musical sense - classical in the class sense.  This convention strongly
# adheres to one component per file (where, ideally, each is a prototype or class).
#
# It expects that each source file defines a component named with `CamelCaps` to be defined in the
# 'global' scope of that module.  No need to deal with `module.exports`!

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
#           Moo: <Moo from moo.js>
#         },
#         Gizmo: <Gizmo from gizmo.js>
#         ManyDoohickey: <ManyDoohickey from many-doohickey.js>
#       },
#       Fizz: <Fizz from fizz.js>
#       FizzBam: <FizzBam from fizz_bam.js>
#     }

Default = require './default'


class Classical extends Default

  fileToProperty: (fileName, parentPath) ->
    baseName = @stripFileExtension fileName

    @specialCaseModuleNames[baseName] or @camelCaps baseName

  modifyExports: (exports, module) ->
    unless module.sandbox[module.id]
      throw new TypeError "Expected #{module.filename} to define #{module.id}"

    module.sandbox[module.id]

module.exports = Classical
