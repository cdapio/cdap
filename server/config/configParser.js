module.exports = {
  extractConfig: extractConfig
};

var promise = require('q'),
    fs = require('fs'),
    spawn = require('child_process').spawn,
    StringDecoder = require('string_decoder').StringDecoder,
    configString = '',
    decoder = new StringDecoder('utf8')
    configJson = null;

/*
 *  Extracts the config based on mode.
 *  @param {string} Current running mode. Enterprise/Developer
 *  @param {string} type of config required. --cConfig for Common Config --sConfig for Security Config
 *  @returns {promise} Returns a promise that gets resolved once the the configs are fetched.
 */

function extractConfig(mode, configParam, isSecure) {
  var deferred = promise.defer(),
      configReader;

  isSecure = isSecure || false;
  if (mode === 'enterprise') {
    if (configJson) {
      return deferred.resolve(configJson);
    }
    configReader = spawn(__dirname + '/../bin/config-tool', ['--' + configParam]);
    configReader.stderr.on('data', configReadFail.bind(this));
    configReader.stdout.on('data', configRead.bind(this));
    configReader.stdout.on('end', onConfigReadEnd.bind(this, deferred, isSecure));
  } else {
    try {
      configJson = require('../../cdap-config.json');
    } catch(e) {
      // Indicates the backend is not running in local environment and that we want only the
      // UI to be running. This is here for convenience.
      console.log('Error!: ', e.code, ' - ', e.message);
      configJson = require('./development/default-config.json');
    }

    deferred.resolve(configJson);
  }
  return deferred.promise;
}

function onConfigReadEnd(deferred, isSecure, data) {
   configJson = JSON.parse(configString);
   deferred.resolve(configJson);
   configString = '';
}

function configRead() {
  var textChunk = decoder.write(arguments[0]);
  if (textChunk) {
    configString += textChunk;
  }
}

function configReadFail() {
  var decoder = new StringDecoder('utf-8');
  var textChunk = decoder.write(arguments[0]);
  if (textChunk) {
    this.logger.error('Extracting the config file failed!');
    this.logger.info(textChunk);
  }
}
