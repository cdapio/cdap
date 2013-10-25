
var assert = require('assert');
var querystring = require('querystring');
var Multipart = require('./multipart');

exports.requestFacebookApi = function(http, host, port, path, params, withMultipart, callback) {
  var req = new FacebookApiRequest(http, host, port, path, params);
  req.start(withMultipart, callback);
};

// export for debug
exports.FacebookApiRequest = FacebookApiRequest;

function bindSelf(self, fn) {
  return function selfBoundFunction() {
    return fn.apply(self, arguments);
  };
}

function FacebookApiRequest(http, host, port, path, params) {
  assert.equal(this.http, null);
  assert.equal(this.host, null);
  assert.equal(this.port, null);
  assert.equal(this.path, null);
  assert.equal(this.params, null);

  // TODO request timeout setting
  // TODO user agent setting

  this.http = http;
  this.host = host;
  this.port = port;
  this.path = path;
  this.params = params;

  this.selfBoundResponseErrorHandler = bindSelf(this, this.handleResponseError);
  this.selfBoundResponseHandler = bindSelf(this, this.handleResponse);
  this.selfBoundDataHandler = bindSelf(this, this.handleData);
  this.selfBoundDataErrorHandler = bindSelf(this, this.handleDataError);
  this.selfBoundEndHandler = bindSelf(this, this.handleEnd);
}

FacebookApiRequest.prototype.http = null;
FacebookApiRequest.prototype.host = null;
FacebookApiRequest.prototype.port = null;
FacebookApiRequest.prototype.path = null;
FacebookApiRequest.prototype.params = null;
FacebookApiRequest.prototype.callback = null;
FacebookApiRequest.prototype.selfBoundResponseErrorHandler = null;
FacebookApiRequest.prototype.selfBoundResponseHandler = null;
FacebookApiRequest.prototype.selfBoundDataHandler = null;
FacebookApiRequest.prototype.selfBoundDataErrorHandler = null;
FacebookApiRequest.prototype.selfBoundEndHandler = null;

FacebookApiRequest.prototype.start = function(withMultipart, callback) {
  assert.equal(this.req, null);
  assert.equal(this.callback, null);

  this.callback = callback;

  if (withMultipart) {
    var multipart = new Multipart();
    var keys = Object.keys(this.params);
    var self = this;
    (function loop() {
      try {
        var key = keys.shift();
        if (key === undefined) {
          afterParams();
          return;
        }
        if (self.params[key].charAt(0) === '@') {
          multipart.addFile(key, self.params[key].substr(1), function(err) {
            if (err) {
              callback(err, null);
            }
            else {
              loop();
            }
          });
        }
        else {
          multipart.addText(key, self.params[key]);
          loop();
        }
      }
      catch (err) {
        callback(err, null);
      }
    })();
    function afterParams() {
      try {
        var options = {
          host: self.host,
          path: self.path,
          port: self.port,
          method: 'POST',
          headers: {
            'Content-Type': multipart.getContentType(),
            'Content-Length': multipart.getContentLength()
          }
        };
        self.req = self.http.request(options);
        self.req.on('error', self.selfBoundResponseErrorHandler);
        self.req.on('response', self.selfBoundResponseHandler);
        multipart.writeToStream(self.req, function(err) {
          if (err) {
            onerror(err);
          }
          else {
            self.req.end();
          }
        });
      }
      catch (err) {
        onerror(err);
      }
      function onerror(err) {
        if (self.req) {
          self.callQuietly(self.detachResponseAndErrorHandlers);
          self.callQuietly(self.abortRequest);
        }
        callback(err, null);
      }
    }
  }
  else {
    // Querystring is encoding multibyte as utf-8.
    var postData = querystring.stringify(this.params);

    var options = {
      host: this.host,
      path: this.path,
      port: this.port,
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': postData.length
      }
    };

    this.req = this.http.request(options);
    this.req.on('error', this.selfBoundResponseErrorHandler);
    this.req.on('response', this.selfBoundResponseHandler);
    this.req.end(postData);
  }
};

FacebookApiRequest.prototype.req = null;

FacebookApiRequest.prototype.handleResponse = function(res) {
  assert.notEqual(this.callback, null);

  try {
    this.detachResponseAndErrorHandlers();
    this.afterResponse(res);
  }
  catch (err) {
    this.callback(err, null);
  }
};

FacebookApiRequest.prototype.handleResponseError = function (err) {
  assert.notEqual(this.callback, null);

  this.callQuietly(this.detachResponseAndErrorHandlers);
  this.callQuietly(this.abortRequest);
  this.callback(err, null);
};

FacebookApiRequest.prototype.detachResponseAndErrorHandlers = function() {
  assert.notEqual(this.req, null);

  this.req.removeListener('error', this.selfBoundResponseErrorHandler);
  this.req.removeListener('response', this.selfBoundResponseHandler);
};

FacebookApiRequest.prototype.afterResponse = function(res) {
  assert.equal(this.res, null);
  assert.equal(this.responseBody, null);

  this.res = res;
  this.res.setEncoding('utf8');
  this.responseBody = [];
  this.res.on('data', this.selfBoundDataHandler);
  this.res.on('error', this.selfBoundDataErrorHandler);
  this.res.on('end', this.selfBoundEndHandler);
};

FacebookApiRequest.prototype.res = null;
FacebookApiRequest.prototype.responseBody = null;

FacebookApiRequest.prototype.handleData = function(data) {
  assert.notEqual(this.responseBody, null);

  this.responseBody.push(data);
};

FacebookApiRequest.prototype.handleDataError = function (err) {
  this.callQuietly(this.detachDataAndEndAndErrorHandlers);
  this.callQuietly(this.abortRequest);
  this.callback(err, null);
};

FacebookApiRequest.prototype.handleEnd = function() {
  assert.notEqual(this.responseBody, null);
  assert.notEqual(this.callback, null);

  try {
    this.detachDataAndEndAndErrorHandlers();
    this.callback(null, this.responseBody.join(''));
  }
  catch (err) {
    this.callback(err, null);
  }
};

FacebookApiRequest.prototype.detachDataAndEndAndErrorHandlers = function() {
  this.res.removeListener('data', this.selfBoundDataHandler);
  this.res.removeListener('error', this.selfBoundDataErrorHandler);
  this.res.removeListener('end', this.selfBoundEndHandler);
};

FacebookApiRequest.prototype.abortRequest = function() {
  assert.notEqual(this.req, null);
  this.req.abort();
};

FacebookApiRequest.prototype.callQuietly = function() {
  try {
    var args = [].slice.call(arguments);
    var fn = args.shift();
    return fn.apply(this, args);
  }
  catch (err) {
    // ignore error
  }
};
