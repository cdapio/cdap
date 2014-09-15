module.exports = {
  extractConfig: extractConfig
};

var promise = require('q'),
    fs = require('fs'),
    lodash = require('lodash'),
    spawn = require('child_process').spawn,
    xml2js = require('xml2js'),
    StringDecoder = require('string_decoder').StringDecoder,
    configObj = {},
    configString = "";

/*
 *  Extracts the config based on mode.
 *  @param {string} Current running mode. Enterprise/Developer
 *  @param {string} type of config required. --cConfig for Common Config --sConfig for Security Config
 *  @returns {promise} Returns a promise that gets resolved once the the configs are fetched.
 */

function extractConfig(mode, configParam) {
  var deferred = promise.defer(),
      decoder = new StringDecoder('utf8'),
      partialConfigRead,
      configReader;
  if (mode === "enterprise") {
    configReader = spawn(__dirname + "/../bin/config-tool", ["--" + configParam]);
    configReader.stderr.on('data', configReadFail.bind(this));
    configReader.stdout.on('data', configRead.bind(this));
    partialConfigRead = lodash.partial(onConfigReadEnd, deferred);
    configReader.stdout.on('end', partialConfigRead.bind(this));
  } else {
    this.config = require("../../cdap-config.json");
    fs.readFile(__dirname + '/../VERSION', "utf-8", function(error, version) {
      if (error) {
        this.logger.info(error);
        this.logger.info("Unable to open VERSION file");
      } else {
        this.config.version = version;
      }
      deferred.resolve();
    }.bind(this));
    if (this.config["ssl.enabled"] === "true") {
      this.config = lodash.extend(this.config, require("../../cdap-security-config.json"));
      if (this.config["dashboard.selfsignedcertificate.enabled"] === "true") {
        /*
          We use mikeal/request library to make xhr request to cdap server.
          In a ssl enabled environment where cdap server uses a self-signed certificate
          node server will fail to connect to cdap server as it is self-signed.
          This environment variable enables that.

          The github issue in relation to this is : https://github.com/mikeal/request/issues/418

          Could not find nodejs doc that discusses about this variable.
        */
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
      }
    }
    this.configSet = true;
  }
  return deferred.promise;
}

function onConfigReadEnd(deferred, data) {
  this.config = lodash.extend(this.config, JSON.parse(configString));
  if (this.config["ssl.enabled"] === "true" && !this.configSet) {
    this.configSet = true;
    configString = "";
    this.extractConfig("enterprise", "security")
        .then(function onSecureConfigComplete() {
          deferred.resolve();
        }.bind(this));
  } else {
    this.configSet = true;
    deferred.resolve();
  }
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
