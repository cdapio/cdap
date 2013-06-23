test = require './common'


subModulesShouldMatch = (opts) ->
  topic: opts.topic

  'we should be given an object with a camelCase key per submodule': (package) ->
    test.assert.keysEqual package, opts.keys

  'each exported property should match the exports of each submodule': (package) ->
    sortedKeys = (k for k of package).sort()
    test.assert.deepEqual (package[k] for k in sortedKeys), ({DAS_MODULE: m} for m in opts.bases.sort())


test.vows.describe('Default Convention').addBatch

  'when we autorequire the example package "fuzzy"': subModulesShouldMatch
    topic: -> test.autorequire('./examples/fuzzy')
    keys:  ['babyThing',  'kitten', 'puppy', 'squidlet']
    bases: ['baby-thing', 'kitten', 'puppy', 'squidlet']

  'when we require the example autorequired package "mixed_tastes"':
    topic: -> require('./examples/mixed_tastes')

    'we should be given an object with a camelCase key per namespace': (package) ->
      test.assert.keysEqual package, ['imbibables', 'legumes', 'meatyGoodness']

    'and we traverse into the "imbibables" namespace': subModulesShouldMatch
      topic: (package) -> package.imbibables
      keys:  ['coffee', 'highlyDistilledCactusJuice',   'tea']
      bases: ['coffee', 'highly_distilled_cactus_juice', 'tea']

    'and we traverse into the "meatyGoodness" namespace': subModulesShouldMatch
      topic: (package) -> package.meatyGoodness
      keys:  ['bacon', 'bloodSausage']
      bases: ['bacon', 'blood-sausage']

  'when we utilize lazy loads':
    topic: -> require('./examples/spaghetti')

    'explicit requires should be honored': (spaghetti) ->
      test.assert.equal spaghetti.main.explicitSimpleDep, 'Simple Dep Stuff'

    'autoloaded dependencies should be honored': (spaghetti) ->
      test.assert.equal spaghetti.main.autoSimpleDep, 'Simple Dep Stuff'

    'autoloadable siblings should be exposed': (spaghetti) ->
      mod = spaghetti.children.kiddo

      test.assert.equal mod.info, 'I am children.kiddo'
      test.assert.equal mod.lazyLoads.bambino().info, 'I am children.bambino'

    'autoloadable ancestors should be exposed': (spaghetti) ->
      mod = spaghetti.children.bambino

      test.assert.equal mod.info, 'I am children.bambino'
      test.assert.equal mod.lazyLoads.main().ohai, 'The flying spaghetti monster was here.'

    'autoloadable grandparents should be exposed': (spaghetti) ->
      mod = spaghetti.children.grandkids.bebe

      test.assert.equal mod.info, 'I am children.grandkids.bebe'
      test.assert.equal mod.lazyLoads.bambino().info, 'I am children.bambino'
      test.assert.equal mod.lazyLoads.main().ohai, 'The flying spaghetti monster was here.'
      test.assert.equal mod.lazyLoads.children().bambino.info, 'I am children.bambino'

    'ambiguous ancestors should cause an error': (spaghetti) ->
      test.assert.throws (-> spaghetti.nephews.bibby), Error
      # Edge case: should not throw because it doesn't reference itsself
      test.assert.doesNotThrow (-> spaghetti.nephews.main), Error

.export(module)
