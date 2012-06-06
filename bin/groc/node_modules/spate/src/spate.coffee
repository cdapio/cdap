# # Spate
#
# Flow control the way I like it.  _One more_ flow control library won't hurt, will it?
spate =

  # ## pool
  #
  # When interacting with system resources such as file handles or sockets, it can be beneficial to
  # throttle the amount of resources you are touching concurrently.  Additionally, it can be used as
  # a debugging aid by throttling what is normally a concurrent process into a squential one.
  #
  # For example:
  #
  #     pool = spate.pool fs.readdirSync('some_dir'), maxConcurrency: 5, (path, done) ->
  #       fs.readFile path, (error, data) ->
  #         return done(error) if error
  #         console.log data
  #         done()
  #
  #     pool.exec (error) ->
  #       console.log 'Finished execution!'
  pool: (workItems, options={}, workerFunc) ->
    # Currently, the only option that this supports is `maxConcurrency`.  It, :gasp:, indicates that
    # there should be no more worker functions active at a time than the value given.  There is no
    # sane default, so it is required.
    #
    # Now, you may be wondering why we're making an _option_ required.
    #
    #  1. It conveys more meaning when implementing a call to `spate.pool`.
    #  2. This function can be extended to support more options without complex argument parsing.
    unless typeof options.maxConcurrency == 'number'
      throw new TypeError 'maxConcurrency is a required option for spate.pool.'

    # For similar reasons as above, we return an object - rather than a wrapped function - for
    # clarity and future extensibility.
    result = {}
    # We only allow the pool to be executed once.
    hasResponded = false

    # You get to call `exec` on it.
    result.exec = (callback) ->
      numInFlight = 0
      itemsLeft   = (i for i in workItems) # Make sure we have an array that we are free to modify.
      # Performance: Because we use an Array, it is considerably faster to pop.  So we reverse it.
      itemsLeft.reverse()

      # Each iteration of work happens in here.
      processItem = ->
        return if hasResponded

        item = itemsLeft.pop()
        if item?
          numInFlight += 1
          # We purposely do not trap exceptions.  They're exceptional; and you, as the implementer,
          # have a much better idea of what to do with them.
          workerFunc item, (error) ->
            numInFlight -= 1
            # Thus, if there is an error, we immediately bail.
            return respond(error) if error?
            # Otherwise, continue on.
            processItem()

        # No item, and we're out of work?  Awesome, we're done!
        else if numInFlight == 0
          respond()

      # Part of the contract is that the callback passed to `exec` is only called once.
      respond = (error) ->
        unless hasResponded
          hasResponded = true
          callback error

      # Kick off enough workers to work through our items.
      while !hasResponded and itemsLeft.length > 0 and numInFlight < options.maxConcurrency
        processItem()

    result


module.exports = spate
