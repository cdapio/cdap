_      = require 'underscore'
assert = require 'assert'
spate  = require '..'
vows   = require 'vows'



poolCommonSuccessVows =
  "should not result in an error": (error) ->
    assert.isUndefined error

  "should perform work on each work item": (error) ->
    assert.deepEqual @result.sort(), [11,12,13,14,15]

  "should not modify the work items": (error) ->
    assert.deepEqual @workItems, [1,2,3,4,5]


vows.describe('spate').addBatch

  "spate.pool":

    "should require maxConcurrency to be set": ->
      assert.throws (-> spate.pool [1,2,3], {}, ->), TypeError
      assert.throws (-> spate.pool [1,2,3], foo: 'bar', ->), TypeError
      assert.throws (-> spate.pool [1,2,3], null, ->), TypeError
      assert.throws (-> spate.pool [1,2,3], undefined, ->), TypeError


    "with synchronous worker functions":
      topic: ->
        (result, options) ->
          @result = []
          @workItems = [1,2,3,4,5]

          pool = spate.pool @workItems, options, (item, done) =>
            @result.push item + 10
            done(result...)

          pool.exec @callback
          undefined

      "and maxConcurrency is less than the number of work items": _(_.clone(poolCommonSuccessVows)).extend
        topic: (work) ->
          work.apply @, [[], maxConcurrency: 2]

      "and maxConcurrency is equal to the number of work items": _(_.clone(poolCommonSuccessVows)).extend
        topic: (work) ->
          work.apply @, [[], maxConcurrency: 5]

      "and maxConcurrency is greater than the number of work items": _(_.clone(poolCommonSuccessVows)).extend
        topic: (work) ->
          work.apply @, [[], maxConcurrency: 10]

      "that finish in error":
        topic: (work) ->
          work.apply @, [['some error'], maxConcurrency: 10]

        "should result in an error": (error, res) ->
          assert.equal error, 'some error'

        "should stop the pool from continuing": (error, res) ->
          assert.deepEqual @result, [11]


    "with asynchronous worker functions":
      topic: ->
        (result, options) ->
          @result = []
          @workItems = [1,2,3,4,5]
          @inProgress = 0
          @maxInProgress = 0

          pool = spate.pool @workItems, options, (item, done) =>
            @inProgress += 1; @maxInProgress = Math.max(@inProgress, @maxInProgress)
            setTimeout (=> @result.push item + 10; @inProgress -= 1; done(result...)), 1

          pool.exec @callback
          undefined

      "and maxConcurrency is less than the number of work items": _(_.clone(poolCommonSuccessVows)).extend
        topic: (work) ->
          work.apply @, [[], maxConcurrency: 2]

        "should cap at maxConcurrency concurrent worker functions": (error) ->
          assert.equal @maxInProgress, 2


      "and maxConcurrency is equal to the number of work items": _(_.clone(poolCommonSuccessVows)).extend
        topic: (work) ->
          work.apply @, [[], maxConcurrency: 5]

        "should cap at maxConcurrency concurrent worker functions": (error) ->
          assert.equal @maxInProgress, 5


      "and maxConcurrency is greater than the number of work items": _(_.clone(poolCommonSuccessVows)).extend
        topic: (work) ->
          work.apply @, [[], maxConcurrency: 10]

        "should spawn as many worker functions as work items": (error) ->
          assert.equal @maxInProgress, 5


      "that finish in error":
        topic: (work) ->
          work.apply @, [['some error'], maxConcurrency: 10]

        "should result in an error": (error, res) ->
          assert.equal error, 'some error'

        "should stop the pool from continuing": (error, res) ->
          assert.deepEqual @result, [11]

.export(module)
