function UrlValidator(cdapConfig) {
  this.whiteListIps = this.getWhiltListIps(cdapConfig);
}


UrlValidator.prototype.isValidURL = function (url) {
  var urlBreakup = this.getUrlBreakup(url);
  var ip = urlBreakup.ipAddress;
  var apiPath = urlBreakup.path;
  var isValid = true;
  if (this.whiteListIps.indexOf(ip) > -1) {
    // check for path
    if (apiPath) {
      if (apiPath.indexOf('http://') > -1 || apiPath.indexOf('https://') > -1 || apiPath.indexOf('ftp://') > -1 || apiPath.indexOf('redirectUrl=') > -1) {
        isValid = false;
      }
    }
  } else {
    isValid = false;
  }
  return isValid;
};

UrlValidator.prototype.getUrlBreakup = function (url) {
  var protocol;
  var hostname;
  var port;
  var path;
  // find  protocol (http, ftp, etc.) and get hostname
  if (url.indexOf('//') > -1) {
    var protocolSplitArr = url.split('//');
    var protoclStr = protocolSplitArr[0];
    if (protoclStr.indexOf(':') > -1) {
      protocol = protoclStr.split(':')[0];
    }

    protocolSplitArr.shift();
    var hostPortPathStr = protocolSplitArr.join('//');

    var hostPortPathSplitArr = hostPortPathStr.split(':');
    hostname = hostPortPathSplitArr.length > 0 ? hostPortPathSplitArr[0] : undefined;
    hostPortPathSplitArr.shift();
    var portPathStr = hostPortPathSplitArr.join(':');

    if (portPathStr && portPathStr !== '') {
      var splitSymbol;

      var slashIndex = portPathStr.indexOf('/');
      var questionIndex = portPathStr.indexOf('?');

      if (slashIndex > -1 && questionIndex > -1) {

        splitSymbol = slashIndex < questionIndex ? '/' : '?';

      } else if (slashIndex > -1 || questionIndex > -1) {

        splitSymbol = slashIndex > -1 ? '/' : '?';
      }

      if (splitSymbol) {
        var portPathSplitArr = portPathStr.split(splitSymbol);
        port = portPathSplitArr[0];
        portPathSplitArr.shift();
        path = portPathSplitArr.join(splitSymbol);
      } else {
        port = portPathStr;
      }
    }
  }
  return { ipAddress: `${protocol}://${hostname}:${port}`, path: path };
};

UrlValidator.prototype.getWhiltListIps = function (config) {
  var whiteList = [];
  if (config) {
    // if user provide any white list from cdap config the it will addpend those ips in whitelisting
    if (config.hasOwnProperty('white.list.ips') && config['white.list.ips'].trim() !== '') {
      whiteList = config['white.list.ips'].trim().split(',');
    }
    // generate whitelist based on cdap config
    var protocol = config['ssl.external.enabled'] === 'true' ? 'https://' : 'http://';
    var port = config['ssl.external.enabled'] === 'true' ? config['router.ssl.server.port'] : config['router.server.port'];
    port = ':' + port;
    var url = [protocol, config['router.server.address'], port].join('');
    whiteList.push(url);
  }
  return whiteList;
};

module.exports = UrlValidator;
