'use strict';


var FsTools = require('../');
var Helper = require('./helper');
var Assert = require('assert');
var Fs = require('fs');

var SANDBOX = Helper.SANDBOX_DIR + '/copy';

require('vows').describe('copy()').addBatch({
  'copying foo to fuu': {
    topic: function () {
      var callback = this.callback;

      FsTools.copy(SANDBOX + '/foo', SANDBOX + '/fuu', function (err) {
        callback(err, SANDBOX + '/fuu');
      });
    },
    'should just work': function (err, dst) {
      Assert.ok(!err, 'Has no errror');
    },
    'creates directory fuu': function (err, dst) {
      Assert.isDirectory(Fs.statSync(dst));
    },
    'creates file fuu/bar/baz/file': function (err, dst) {
      Assert.isFile(Fs.statSync(dst + '/bar/baz/file'));
    },
    'creates symlink fuu/bar/baz/link': function (err, dst) {
      Assert.isSymbolicLink(Fs.lstatSync(dst + '/bar/baz/link'));
    },
    'creates symlink fuu/bar/baz/link pointing to directory': function (err, dst) {
      Assert.isDirectory(Fs.statSync(dst + '/bar/baz/link'));
    },
    'after all': {
      topic: function () {
        var callback = this.callback, find;

        find = 'find ' + SANDBOX + ' -mindepth 1 -printf %y';

        require('child_process').exec(find, function (err, stdout) {
          callback(err, {
            total: stdout.length,
            files: stdout.match(/f/g).length,
            dirs: stdout.match(/d/g).length,
            syms: stdout.match(/l/g).length
          });
        });
      },
      'makes shallow copy of src': function (err, result) {
        Assert.ok(!err, 'Has no error');
        Assert.equal(result.total, 20);
        Assert.equal(result.files, 7);
        Assert.equal(result.dirs, 6);
        Assert.equal(result.syms, 7);
      },
    },
  },
}).export(module);
