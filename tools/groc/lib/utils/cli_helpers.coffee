# # Command Line Helpers
CLIHelpers =

  # ## configureOptimist

  # [Optimist](https://github.com/substack/node-optimist) fails to provide a few conveniences, so we
  # layer on a little bit of additional structure when defining our options.
  configureOptimist: (opts, config, extraDefaults) ->
    # * We want the notion of a canonical option name; which we specify via the keys of `config`.
    for optName, optConfig of config
      opts.options optName,
        describe: optConfig.description
        alias:    optConfig.aliases

    # * Optimist's `options()` method doesn't honor aliased values when specifying boolean values,
    #   so we are forced to set those up separately.
    for optName, optConfig of config
      if optConfig.type == 'boolean'
        opts.boolean _.compact _.flatten [optName, optConfig.aliases]

    # * We support two tiers of default values.  First, we set up the hard-coded defaults specified
    #   as part of `config`.
    defaults = {}
    for optName, optConfig of config
      defaults[optName] = optConfig.default if optConfig.default?

    # * We follow up with the higher precedence defaults passed via `extraDefaults` (such as
    #   persisted options)
    defaults = _(defaults).extend extraDefaults

    for optName, value of defaults
      # * We want the ability to specify reactionary default values, so that the user can inspect
      #   the current state of things by tacking on a `--help`.
      value = value opts if _.isFunction value

      opts.default optName, value


  # ## extractArgv

  # In addition to the extended configuration that we desire, we also want special handling for
  # generated values:
  extractArgv: (opts, config) ->
    argv = opts.argv

    # * With regular optimist parsing, you either get an individual value or an array.  For
    #   list-style options, we always want an array.
    for optName, optConfig of config
      if optConfig.type == 'list' and not _.isArray opts.argv[optName]
        argv[optName] = _.compact [ argv[optName] ]

    # * It's also handy to auto-resolve paths.
    for optName, optConfig of config
      argv[optName] = path.resolve argv[optName] if optConfig.type == 'path'

    argv


  # ## guessPrimaryGitHubURL

  guessPrimaryGitHubURL: (callback) ->
    # `git config --list` provides information about branches and remotes - everything we need to
    # attempt to guess the project's GitHub repository.
    #
    # There are several states that a GitHub-based repository could be in, and we've probably missed
    # a few.  We attempt to guess it through a few means:
    childProcess.exec 'git config --list', (error, stdout, stderr) =>
      return error if error

      config = {}
      for line in stdout.split '\n'
        pieces = line.split '='
        config[pieces[0]] = pieces[1]

      # * If the user has a tracked `gh-pages` branch, chances are extremely high that its tracked
      #   remote is the correct github project.
      pagesRemote = config['branch.gh-pages.remote']
      if pagesRemote and config["remote.#{pagesRemote}.url"]
        url = @extractGitHubURL config["remote.#{pagesRemote}.url"]
        return callback null, url if url

      # * If that fails, we fall back to the origin remote if it is a GitHub repository.
      url = @extractGitHubURL config['remote.origin.url']
      return callback null, url if url

      # * We fall back to searching all remotes for a GitHub repository, and choose the first one
      #   we encounter.
      for key, value of config
        url = @extractGitHubURL value
        return callback null, url if url

      callback new Error "Could not guess a GitHub URL for the current repository :("

  # A quick helper that extracts a GitHub project URL from its repository URL.
  extractGitHubURL: (url) ->
    match = url?.match /github\.com:([^\/]+)\/([^\/]+)\.git/
    return null unless match

    "https://github.com/#{match[1]}/#{match[2]}"
