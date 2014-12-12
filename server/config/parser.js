/*global require, module, process */

module.exports = {
  extractConfig: extractConfig
};

var promise = require('q'),
    fs = require('fs'),
    spawn = require('child_process').spawn,
    StringDecoder = require('string_decoder').StringDecoder,
    decoder = new StringDecoder('utf8'),
    cache = {},
    buffer = '';


/*
 *  Extracts the config
 *  @returns {promise}
 */

function extractConfig(param) {
  var deferred = promise.defer(),
      tool;

  param = param || 'cdap';

  if (cache[param]) {
    deferred.resolve(cache[param]);
    return deferred.promise;
  }

  if (process.env.CDAP_MODE === 'enterprise') {
    buffer = '';
    tool = spawn(__dirname + '/../../bin/config-tool', ['--'+param]);
    tool.stderr.on('data', configReadFail.bind(this));
    tool.stdout.on('data', configRead.bind(this));
    tool.stdout.on('end', onConfigReadEnd.bind(this, deferred, param));
  } else {
    try {
      cache[param] = require('../../cdap-config.json');
    } catch(e) {
      // Indicates the backend is not running in local environment and that we want only the
      // UI to be running. This is here for convenience.
      console.error('!!! using development configuration for', '"'+param+'"');
      cache[param] = require('./development/'+param+'.json');
    }

    deferred.resolve(cache[param]);
  }
  return deferred.promise;
}

function onConfigReadEnd (deferred, param) {
   cache[param] = JSON.parse(buffer);
   deferred.resolve(cache[param]);
}

function configRead (data) {
  var textChunk = decoder.write(data);
  if (textChunk) {
    buffer += textChunk;
  }
}

function configReadFail (data) {
  var textChunk = decoder.write(data);
  if (textChunk) {
    console.error('Failed to extract the configuration!');
    console.log(textChunk);
  }
}

