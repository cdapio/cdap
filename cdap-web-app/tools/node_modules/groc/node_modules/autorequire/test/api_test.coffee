test = require './common'


test.vows.describe('autorequire API').addBatch
  'conventions':
    topic: -> test.autorequire.conventions

    'should be exposed as classes following the CamelCaps scheme': (conventions) ->
      test.assert.keysEqual conventions, ['Classical', 'Default', 'Ruby']

  'when overriding with an implicit Default convention':
    topic: ->
      test.autorequire './examples/mixed_tastes/lib',
        fileToProperty:      (fileName, parentPath)      -> fileName.toUpperCase()
        directoryToProperty: (directoryName, parentPath) -> directoryName.toUpperCase()

    'directoryToProperty should be overridden': (package) ->
      test.assert.keysEqual package, ['IMBIBABLES', 'LEGUMES.COFFEE', 'MEATY_GOODNESS']

    'fileToProperty should be overridden': (package) ->
      test.assert.keysEqual package.IMBIBABLES, ['COFFEE.COFFEE', 'HIGHLY_DISTILLED_CACTUS_JUICE.JS', 'TEA.JS']
      test.assert.keysEqual package.MEATY_GOODNESS, ['BACON.JS', 'BLOOD-SAUSAGE.COFFEE']

  'when overriding with an explicit Default convention':
    topic: ->
      test.autorequire './examples/mixed_tastes/lib', 'Default'
        fileToProperty:      (fileName, parentPath)      -> fileName.toUpperCase()
        directoryToProperty: (directoryName, parentPath) -> directoryName.toUpperCase()

    'directoryToProperty should be overridden': (package) ->
      test.assert.keysEqual package, ['IMBIBABLES', 'LEGUMES.COFFEE', 'MEATY_GOODNESS']

    'fileToProperty should be overridden': (package) ->
      test.assert.keysEqual package.IMBIBABLES, ['COFFEE.COFFEE', 'HIGHLY_DISTILLED_CACTUS_JUICE.JS', 'TEA.JS']
      test.assert.keysEqual package.MEATY_GOODNESS, ['BACON.JS', 'BLOOD-SAUSAGE.COFFEE']

  'when overriding with an explicit non-Default convention':
    topic: ->
      test.autorequire './examples/mixed_tastes/lib', 'Ruby'
        fileToProperty: (fileName, parentPath) -> fileName.toUpperCase()

    'directoryToProperty should be overridden': (package) ->
      test.assert.keysEqual package, ['imbibables', 'LEGUMES.COFFEE', 'meaty_goodness']

    'fileToProperty should be overridden': (package) ->
      test.assert.keysEqual package.imbibables, ['COFFEE.COFFEE', 'HIGHLY_DISTILLED_CACTUS_JUICE.JS', 'TEA.JS']
      test.assert.keysEqual package.meaty_goodness, ['BACON.JS', 'BLOOD-SAUSAGE.COFFEE']

.export module
