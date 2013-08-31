'use strict';


var FsTools = require('../');
var Helper = require('./helper');
var Assert = require('assert');
var Fs = require('fs');

var SANDBOX = Helper.SANDBOX_DIR + '/mkdir';

require('vows').describe('mkdir()').addBatch({
  'when destination can be created': {
    topic: function () {
      var callback = this.callback, path = SANDBOX + '/test';

      FsTools.mkdir(path, '0711', function (err) {
        callback(err, path);
      });
    },
    'should create directory with requested permissions': function (err, path) {
      Assert.ok(!err, 'Has no errror');

      var stats = Fs.statSync(path);

      Assert.isDirectory(stats);
      Assert.hasPermsMode(stats, '0711');
    }
  },
  'when can not create directory, due to permissions of parent': {
    topic: function () {
      // TODO: Add chek if current user is root, and if so - skip test
      FsTools.mkdir('/FOOBAR-FS-TOOLS', this.callback);
    },
    'can\'t create under /etc': function (err, result) {
      Assert.instanceOf(err, Error);
      Assert.equal(err.code, 'EACCES');
    }
  }
}).export(module);
