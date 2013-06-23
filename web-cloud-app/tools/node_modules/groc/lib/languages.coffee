# # Supported Languages

LANGUAGES =
  Markdown:
    nameMatchers: ['.md']
    commentsOnly: true

  C:
    nameMatchers:      ['.c', '.h']
    pygmentsLexer:     'c'
    singleLineComment: ['//']

  'C++':
    nameMatchers:      ['.cpp', '.hpp', '.c++', '.h++', '.cc', '.hh', '.cxx', '.hxx']
    pygmentsLexer:     'cpp'
    singleLineComment: ['//']

  CoffeeScript:
    nameMatchers:      ['.coffee']
    pygmentsLexer:     'coffee-script'
    singleLineComment: ['#']

  Haskell:
    nameMatchers:      ['.hs']
    pygmentsLexer:     'haskell'
    singleLineComment: ['--']

  Jade:
    nameMatchers:      ['.jade']
    pygmentsLexer:     'jade'
    singleLineComment: ['//']

  Java:
    nameMatchers:      ['.java']
    pygmentsLexer:     'java'
    singleLineComment: ['//']

  JavaScript:
    nameMatchers:      ['.js']
    pygmentsLexer:     'javascript'
    singleLineComment: ['//']

  Jake:
    nameMatchers:      ['.jake']
    pygmentsLexer:     'javascript'
    singleLineComment: ['//']

  LaTeX:
    nameMatchers:      ['.tex', '.latex', '.sty']
    pygmentsLexer:     'latex'
    singleLineComment: ['%']

  'Objective-C':
    nameMatchers:      ['.m', '.mm']
    pygmentsLexer:     'objc'
    singleLineComment: ['//']

  Perl:
    nameMatchers:      ['.pl', '.pm']
    pygmentsLexer:     'perl'
    singleLineComment: ['#']

  PHP:
    nameMatchers:      [/\.php\d?$/, '.fbp']
    pygmentsLexer:     'php'
    singleLineComment: ['//']

  Python:
    nameMatchers:      ['.py']
    pygmentsLexer:     'python'
    singleLineComment: ['#']

  Ruby:
    nameMatchers:      ['.rb']
    pygmentsLexer:     'ruby'
    singleLineComment: ['#']

  Sass:
    nameMatchers:      ['.sass']
    pygmentsLexer:     'sass'
    singleLineComment: ['//']

  SCSS:
    nameMatchers:      ['.scss']
    pygmentsLexer:     'scss'
    singleLineComment: ['//']

  Shell:
    nameMatchers:      ['.sh']
    pygmentsLexer:     'sh'
    singleLineComment: ['#']

  YAML:
    nameMatchers:      ['.yml', '.yaml']
    pygmentsLexer:     'yaml'
    singleLineComment: ['#']
