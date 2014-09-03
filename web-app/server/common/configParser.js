module.exports = {
  extractConfigFromXml: extractConfigFromXml,
  extractBaseConfig: extractBaseConfig
};

var promise = require('q'),
    spawn = require('child_process').spawn,
    xml2js = require('xml2js'),
    StringDecoder = require('string_decoder').StringDecoder,
    util = require('./util'),
    configObj = {};

function extractConfigFromXml(filename) {
  var deferredObj = promise.defer(),
      promises = [];

  //There should definitely be better way to do this.
  util.readFile(filename)
    .then(parseConfigXmlFile.bind(this))
    .then(parseCredentials.bind(this))
    .then( function onConfigCredentialFetch(error, apiKey) {
      configObj.apiKey = apiKey;
      configObj.configSet = true;
      deferredObj.resolve(configObj);
    }.bind(this));
  return deferredObj.promise;
}

function extractBaseConfig() {
  var deferred = promise.defer(),
      decoder = new StringDecoder('utf8'),
      xmlReader = spawn("jar", ['-xvf', __dirname + '/lib/common-2.3.1.jar', 'continuuity-default.xml']);

  xmlReader.stderr.on('data', xmlReadFail);
  xmlReader.stdout.on('end', function onXmlReadEnd(data) {
    util.readFile('continuuity-default.xml')
      .then( function onXmlFetch(result, error) {
        return parseXml(result);
      })
      .then( function onConfigFetch(config) {
        this.baseConfig = config;
        this.isDefaultConfig = true;
        deferred.resolve();
      }.bind(this))
  }.bind(this));
  return deferred.promise;
}

function parseXml(xml) {
  var deferred = promise.defer(),
      config = {},
      item,
      parser = new xml2js.Parser();

  parser.parseString(xml, function onXmlBufferParse(err, result) {
    result = result.property;
    for (item in result) {
      item = result[item];
      config[item.name] = item.value;
    }
    deferred.resolve(config);
  });
  return deferred.promise;
}

function parseConfigXmlFile (result, error) {
  parseXml(result)
    .then(function (configuration) {
      configObj = configuration;
    })
  return  util.readFile(__dirname + '/../../../VERSION', 'utf-8');
}

function parseCredentials(v) {
  configObj.version = v;
  return util.readFile(__dirname + '/.credential', 'utf-8');
}

function xmlReadFail() {
  var textChunk = decoder.write(arguments[0]);
  if (textChunk) {
    this.logger.info(textChunk);
  } else {
    this.logger.error('Extracting the xml file failed!');
  }
}