# Autorequire [![Build Status](https://secure.travis-ci.org/nevir/node-autorequire.png)](http://travis-ci.org/nevir/node-autorequire)

Autorequire provides a means of defining a consistent file and directory structure for your Node.js
packages.  It does not force any one structure upon you - instead, it provides common
conventions, and the ability to define your own if they do not suit.

At its heart, autorequire is an extensible replacement for Node's `require()`.  It also provides a
simple way of navigating a node package (from within and out), using the package's directory
structure as a guide, and the convention to assist in naming modules.

When interacting with an autorequired Node package, each module and directory is lazy-loaded and
then memoized upon reference.  This ensures that it is a minimal performance hit.


## Usage

To use autorequire for your package, at its most basic, is just the following in your index:

    module.exports = require('autorequire')('./lib');

That's it!  You no longer need to require the core Node modules in any of your project's source
files, nor do they need to require each other.  Consumers of your Node.js package require it
normally, and should not notice a difference.


## Defining Custom Conventions

Should the default convention not suit your needs, there are [several more defined]
(https://github.com/nevir/node-autorequire/tree/master/src/conventions).  You can specify a
built-in convention by passing its name as the second argument to autorequire:

    module.exports = require('autorequire')('./lib', 'Classical');

Or, should you want to override a specific piece of the convention, you can inherit from the default
convention.  Or, for ease, you can simply pass a hash of instance methods, and autorequire will
manage the inheritance for you:

    module.exports = require('autorequire')('./lib', {
      directoryToProperty: function(directoryName, parentPath) {
        return this.underscore(directoryName);
      }
    });

For a full reference of the methods that a convention can define, see the docs for the default
convention.


## Notes

__The caveat__ to autorequire is that every autorequired module is loaded within a sandboxed
environment via `vm`'s `runInNewContext`.  This is so that lazy-loaded modules do not pollute the
global context for everyone.  It's also a good practice for Node.js projects to adhere to.
