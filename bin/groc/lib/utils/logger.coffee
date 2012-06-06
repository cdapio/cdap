# # groc.Logger

# We have pretty simple needs for a logger, and so far have been unable to find a reasonable
# off-the-shelf solution that fits them without being too overbearing:
class Logger
  # * We want the standard levels of output, plus a few more.
  LEVELS:
    TRACE: 0
    DEBUG: 1
    INFO:  2
    PASS:  2
    WARN:  3
    ERROR: 4

  # * Full on level: labels are **extremely** heavy for what is primarily a command-line tool.  The
  #   common case - `INFO` - does not even expose a label.  We only want it to call out uncommon
  #   events with some slight symbolism.
  LEVEL_PREFIXES:
    TRACE: '∴ '
    DEBUG: '‡ '
    INFO:  '  '
    PASS:  '✓ '
    WARN:  '» '
    ERROR: '! '

  # * Colors make the world better.
  LEVEL_COLORS:
    TRACE: 'grey'
    DEBUG: 'grey'
    INFO:  'black'
    PASS:  'green'
    WARN:  'yellow'
    ERROR: 'red'

  # * Don't forget the semantics of our output.
  LEVEL_STREAMS:
    TRACE: console.log
    DEBUG: console.log
    INFO:  console.log
    PASS:  console.log
    WARN:  console.error
    ERROR: console.error

  constructor: (minLevel = @LEVELS.INFO) ->
    @minLevel = minLevel

    for name of @LEVELS
      do (name) =>
        @[name.toLowerCase()] = (args...) ->
          @emit name, args...

  emit: (levelName, args...) ->
    if @LEVELS[levelName] >= @minLevel
      output = utils.CompatibilityHelpers.format args...

      # * We like nicely indented output
      output = output.split(/\r?\n/).join('\n  ')

      @LEVEL_STREAMS[levelName] colors[@LEVEL_COLORS[levelName]] "#{@LEVEL_PREFIXES[levelName]}#{output}"

      output

# * Sometimes we just want one-off logging
globalLogger = new Logger Logger::LEVELS.TRACE

for level of globalLogger.LEVELS
  do (level) ->
    Logger[level.toLowerCase()] = (args...) -> globalLogger[level.toLowerCase()] args...
