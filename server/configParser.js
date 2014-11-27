module.exports = {
  extractConfig: extractConfig
};

var promise = require('q'),
    fs = require('fs'),
    lodash = require('lodash'),
    spawn = require('child_process').spawn,
    StringDecoder = require('string_decoder').StringDecoder,
    configString = "";

/*
 *  Extracts the config based on mode.
 *  @param {string} Current running mode. Enterprise/Developer
 *  @param {string} type of config required. --cConfig for Common Config --sConfig for Security Config
 *  @returns {promise} Returns a promise that gets resolved once the the configs are fetched.
 */

function extractConfig(mode, configParam, isSecure) {
  var deferred = promise.defer(),
      decoder = new StringDecoder('utf8'),
      partialConfigRead,
      configReader;
  isSecure = isSecure || false;
  if (mode === "enterprise") {
    configReader = spawn(__dirname + "/../bin/config-tool", ["--" + configParam]);
    configReader.stderr.on('data', configReadFail.bind(this));
    configReader.stdout.on('data', configRead.bind(this));
    configReader.stdout.on('end', onConfigReadEnd.bind(this, deferred, isSecure));
  }
  return deferred.promise;
}

function onConfigReadEnd(deferred, isSecure, data) {
//  if (this.config["ssl.enabled"] === "true") {
//    configString = "";
//    this.extractConfig("enterprise", "security", true)
//        .then(function onSecureConfigComplete() {
//          deferred.resolve();
//        }.bind(this));
//  } else {
   deferred.resolve(JSON.parse(configString));
  //}
}

function configRead() {
  var decoder = new StringDecoder('utf-8');
  var textChunk = decoder.write(arguments[0]);
  if (textChunk) {
    configString += textChunk;
  } else {
    this.logger.error('Extracting the config file failed!');
  }
}

function configReadFail() {
  var decoder = new StringDecoder('utf-8');
  var textChunk = decoder.write(arguments[0]);
  if (textChunk) {
    this.logger.info(textChunk);
  } else {
    this.logger.error('Extracting the config file failed!');
  }
}