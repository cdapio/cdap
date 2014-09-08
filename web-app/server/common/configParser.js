module.exports = {
  extractConfig: extractConfig
};

var promise = require('q'),
    spawn = require('child_process').spawn,
    xml2js = require('xml2js'),
    StringDecoder = require('string_decoder').StringDecoder,
    configObj = {};

function extractConfig(mode) {
  var deferred = promise.defer(),
      decoder = new StringDecoder('utf8'),
      configReader;
  if (mode === "enterprise") {
    configReader = spawn(__dirname + "/../bin/config-tool" ,["--output", "/tmp/cdap-config.json"]);
    configReader.stderr.on('data', configReadFail.bind(this));
    configReader.stdout.on('end', function onXmlReadEnd(data) {
      this.config = require("/tmp/cdap-config.json");
      this.configSet = true;
      deferred.resolve();
    }.bind(this));
  } else {
    this.config = require("../../cdap-config.json");
    this.configSet = true;
    deferred.resolve();
  }
  return deferred.promise;
}

function configReadFail() {
  var decoder = new (require('string_decoder').StringDecoder)('utf-8');
  var textChunk = decoder.write(arguments[0]);
  if (textChunk) {
    this.logger.info(textChunk);
  } else {
    this.logger.error('Extracting the config file failed!');
  }
}