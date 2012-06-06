# groc

Groc takes your _documented_ code, and in an admission that people aren't machines, generates
documentation that follows the spirit of literate programming.  Take a look at the
[self-generated documentation](http://nevir.github.com/groc/), and see if it appeals to you!

It is very heavily influenced by [Jeremy Ashkenas](https://github.com/jashkenas)'
[docco](http://jashkenas.github.com/docco/), and is an attempt to further enhance the idea (thus,
groc can't tout the same quick 'n dirty principles of docco).


## What does it give you?

Groc will:

* Generate documentation from your source code, by displaying your
  [Markdown](http://daringfireball.net/projects/markdown/) formatted comments next to the code
  fragments that they document.

* Submit your project's documentation to the [github pages](http://pages.github.com/) for your
  project.

* Generate a searchable table of contents for all documented files and headers within your project.

* Gracefully handle complex hierarchies of source code across multiple folders.

* Read a configuration file so that you don't have to think when you want your documentation built;
  you just type `groc`.


## How?

### Installing groc

Groc depends on [Node.js](http://nodejs.org/) and [Pygments](http://pygments.org/).  Once you have
those installed - and assuming that your node install came with [npm](http://npmjs.org/) - you can
install groc via:

    npm install -g groc

For those new to npm, `-g` indicates that you want groc installed as a global command for your
environment.  You may need to prefix the command with sudo, depending on how you installed node.


### Using groc (CLI)

To generate documentation, just point groc to source files that you want docs for:

    groc *.rb

Groc will also handle extended globbing syntax if you quote arguments:

    groc "lib/**/*.coffee" README.md

By default, groc will drop the generated documentation in the `doc/` folder of your project, and it
will treat `README.md` as the index.  Take a look at your generated docs, and see if everything is
in order!

Once you are pleased with the output, you can push your docs to your github pages branch:

    groc --github "lib/**/*.coffee" README.md

Groc will automagically create and push the `gh-pages` branch if it is missing.

There are [additional options](http://nevir.github.com/groc/cli.html#cli-options) supported by
groc, if you are interested.


### Configuring groc (.groc.json)

Groc supports a simple JSON configuration format once you know the config values that appeal to you.

Create a `.groc.json` file in your project root, where each key maps to an option you would pass to
the `groc` command.  File names and globs are defined as an array with the key `globs`.  For
example, groc's own configuration is:

    {
      "globs": ["lib/**/*.coffee", "README.md", "lib/styles/*/style.sass", "lib/styles/*/*.jade"],
      "github": true
    }

From now on, if you call `groc` without any arguments, it will use your pre-defined configuration.


## Literate programming?

[Literate programming](http://en.wikipedia.org/wiki/Literate_programming) is a programming
methodology coined by [Donald Knuth](http://en.wikipedia.org/wiki/Donald_Knuth).  The primary tenet
is that you write a program so that the structure of both the code and documentation align with
your mental model of its behaviors and processes.

Groc aims to provide a happy medium where you can freely write your source files as structured
documents, while not going out of your way to restructure the code to fit the documentation.
Here are some suggested guidelines to follow when writing your code:

* Try to keep the size of each source file down.  It is helpful if each file fulfills a specific
  feature of your application or library.

* Rather than commenting individual lines of code, write comments that explain the _behavior_ of a
  given method or code block.  Take advantage of the fact that comments can span that method.

* Make gratuitous use of lists when explaining processes; step by step explanations are extremely
  easy to follow!

* Break each source file into sections via headers.  Don't be afraid to split source into even
  smaller files if it makes them more readable.

Writing documentation is _hard_; hopefully groc helps to streamline the process for you!


## Known Issues

* Groc does not fare well with files that have very long line lengths (minimized
  JavaScript being the prime offender).  Make sure that you exclude them!


## What's in the works?

Groc wants to:

* [Fully support hand-held viewing of documentation](https://github.com/nevir/groc/issues/1).  It
  can almost do this today, but the table of contents is horribly broken in the mobile view.
