test = require './common'


process.env['convention'] = 'Classical'

subModulesShouldMatch = (opts) ->
  topic: opts.topic

  'we should be given an object with a CamelCaps key per submodule': (package) ->
    test.assert.keysEqual package, opts.keys

  'each exported property should match the exports of each submodule': (package) ->
    test.assert.deepEqual (v[0] for k,v of package).sort(), opts.keys.sort()

test.vows.describe('Classical Convention').addBatch

  'when we autorequire the example package "fuzzy"': subModulesShouldMatch
    topic: -> test.autorequire('./examples/fuzzy', 'Classical')
    keys:  ['BabyThing', 'Kitten', 'Puppy', 'Squidlet']

  'when we require the example autorequired package "mixed_tastes"':
    topic: -> require('./examples/mixed_tastes')

    'we should be given an object with a camelCase key per namespace': (package) ->
      test.assert.keysEqual package, ['imbibables', 'Legumes', 'meatyGoodness']

    'and we traverse into the "imbibables" namespace': subModulesShouldMatch
      topic: (package) -> package.imbibables
      keys:  ['Coffee', 'HighlyDistilledCactusJuice', 'Tea']

    'and we traverse into the "meatyGoodness" namespace': subModulesShouldMatch
      topic: (package) -> package.meatyGoodness
      keys:  ['Bacon', 'BloodSausage']

.export(module)
