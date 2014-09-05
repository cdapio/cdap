module.exports = {
  extractConfig: extractConfig
};

var promise = require('q'),
    spawn = require('child_process').spawn,
    xml2js = require('xml2js'),
    StringDecoder = require('string_decoder').StringDecoder,
    util = require('./util'),
    configObj = {};

function extractConfig(mode) {
  var deferred = promise.defer(),
      decoder = new StringDecoder('utf8'),
      configReader;
  if (mode === "enterprise") {
    configReader = spawn("../../bin/config-tool", ['--output ../../cdap-config.json']);
    configReader.stderr.on('data', configReadFail.bind(this));
    configReader.stdout.on('end', function onXmlReadEnd(data) {
      this.config = require("../../cdap-config.json");
      this.configSet = true;
      deferred.resolve();
    });
  } else {
    this.config = require("../../cdap-config.json");
    this.configSet = true;
    setTimeout(function() {
      deferred.resolve();
    }, 10)
  }
  return deferred.promise;
}

function xmlReadFail() {
  var decoder = new (require('string_decoder').StringDecoder)('utf-8');
  var textChunk = decoder.write(arguments[0]);
  if (textChunk) {
    this.logger.info(textChunk);
  } else {
    this.logger.error('Extracting the config file failed!');
  }
}