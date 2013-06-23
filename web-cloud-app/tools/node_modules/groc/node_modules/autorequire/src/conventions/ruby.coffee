# The same as the classical convention, except that directories in the module hierarchy are defined
# with `under_scores`.  This helps to differentiate them as namespaces.

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
#       things_and_baubles: {
#         stuff: {
#           Moo: <Moo from moo.js>
#         },
#         Gizmo: <Gizmo from gizmo.js>
#         ManyDoohickey: <ManyDoohickey from many-doohickey.js>
#       },
#       Fizz: <Fizz from fizz.js>
#       FizzBam: <FizzBam from fizz_bam.js>
#     }

Classical = require './classical'


class Ruby extends Classical
  directoryToProperty: (directoryName, parentPath) ->
    @underscore directoryName

module.exports = Ruby
