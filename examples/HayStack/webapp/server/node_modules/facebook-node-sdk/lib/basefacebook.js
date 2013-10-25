var assert = require('assert');
var https = require('https');
var crypto = require('crypto');
var util = require('util');
var url = require('url');
var querystring = require('querystring');
var cb = require('./cbutil');
var requestUtil = require('./requestutil');

/**
 * Initialize a Facebook Application.
 *
 * The configuration:
 * - appId: the application ID
 * - secret: the application secret
 * - fileUpload: (optional) boolean indicating if file uploads are enabled
 *
 * @param array $config The application configuration
 */
function BaseFacebook(config) {
  if (config.hasOwnProperty('request')) {
    this.request = config.request;
  }
  if (config.hasOwnProperty('response')) {
    this.response = config.response;
  }
  if (config.hasOwnProperty('currentUrl')) {
    this.currentUrl = config.currentUrl;
  }
  this.setAppId(config.appId);
  this.setAppSecret(config.secret);
  if (config.hasOwnProperty('fileUpload')) {
    this.setFileUploadSupport(config.fileUpload);
  }

  var state = this.getPersistentData('state');
  if (state) {
    this.state = state;
  }
}

BaseFacebook.prototype.request = null;
BaseFacebook.prototype.response = null;

BaseFacebook.prototype.currentUrl = null;

BaseFacebook.prototype.sessionNameMap = {
  access_token: 'access_token',
  user_id: 'user_id',
  code: 'code',
  state: 'state'
};

BaseFacebook.prototype.getRequestParam = function(key) {
  if (!this.request) {
    return null;
  }
  if (this.request.query && this.request.query.hasOwnProperty(key)) {
    return this.request.query[key];
  }
  else if (this.request.body && this.request.body.hasOwnProperty(key)) {
    return this.request.body[key];
  }
  else {
    return null;
  }
};

BaseFacebook.prototype.getCookie = function(key) {
  if (this.hasCookie(key)) {
    return this.request.cookies[key];
  }
  else {
    return null;
  }
};

BaseFacebook.prototype.hasCookie = function(key) {
  return this.request && this.request.cookies && this.request.cookies.hasOwnProperty(key);
};

BaseFacebook.prototype.sentHeaders = function() {
  return this.response && this.response._header;
};

BaseFacebook.prototype.clearCookie = function(key, options) {
  if (this.response) {
    this.response.clearCookie.apply(this.response, arguments);
  }
};

/*
We don't need yet.
BaseFacebook.prototype.setCookie = function(key, value, options) {
  if (this.response) {
    this.response.cookie.apply(this.response, arguments);
  }
};
*/

BaseFacebook.prototype.appId = null;

/**
 * Set the Application ID.
 *
 * @param string appId The Application ID
 * @return BaseFacebook
 */
BaseFacebook.prototype.setAppId = function(appId) {
  this.appId = appId;
  return this;
};

/**
 * Get the Application ID.
 *
 * @return string the Application ID
 */
BaseFacebook.prototype.getAppId = function() {
  return this.appId;
};

BaseFacebook.prototype.appSecret = null;

/**
 * Set the App Secret.
 *
 * @param string appSecret The App Secret
 * @return BaseFacebook
 * @deprecated
 */
BaseFacebook.prototype.setApiSecret = function(appSecret) {
  this.appSecret = appSecret;
  return this;
};

/**
 * Set the App Secret.
 *
 * @param string appSecret The App Secret
 * @return BaseFacebook
 */
BaseFacebook.prototype.setAppSecret = function(appSecret) {
  this.appSecret = appSecret;
  return this;
};

/**
 * Get the App Secret.
 *
 * @return string the App Secret
 * @deprecated
 */
BaseFacebook.prototype.getApiSecret = function() {
  return this.appSecret;
};

/**
 * Get the App Secret.
 *
 * @return string the App Secret
 */
BaseFacebook.prototype.getAppSecret = function() {
  return this.appSecret;
};

BaseFacebook.prototype.fileUploadSupport = false;

/**
 * Set the file upload support status.
 *
 * @param boolean $fileUploadSupport The file upload support status.
 * @return BaseFacebook
 */
BaseFacebook.prototype.setFileUploadSupport = function(fileUploadSupport) {
  this.fileUploadSupport = fileUploadSupport;
  return this;
};

/**
 * Get the file upload support status.
 *
 * @return boolean true if and only if the server supports file upload.
 */
BaseFacebook.prototype.getFileUploadSupport = function() {
  return this.fileUploadSupport;
};


/**
 * DEPRECATED! Please use getFileUploadSupport instead.
 *
 * Get the file upload support status.
 *
 * @return boolean true if and only if the server supports file upload.
 */
BaseFacebook.prototype.useFileUploadSupport = function() {
  return this.getFileUploadSupport();
}


BaseFacebook.prototype.accessToken = null;

/**
 * Sets the access token for api calls.  Use this if you get
 * your access token by other means and just want the SDK
 * to use it.
 *
 * @param string $access_token an access token.
 * @return BaseFacebook
 */
BaseFacebook.prototype.setAccessToken = function(accessToken) {
  this.accessToken = accessToken;
  return this;
};

/**
 * Determines the access token that should be used for API calls.
 * The first time this is called, $this->accessToken is set equal
 * to either a valid user access token, or it's set to the application
 * access token if a valid user access token wasn't available.  Subsequent
 * calls return whatever the first call returned.
 *
 * @return string The access token
 */
BaseFacebook.prototype.getAccessToken = function getAccessToken(callback) {
  if (this.accessToken !== null) {
    // we've done this already and cached it.  Just return.
    callback(null, this.accessToken);
  }
  else {
    // first establish access token to be the application
    // access token, in case we navigate to the /oauth/access_token
    // endpoint, where SOME access token is required.
    this.setAccessToken(this.getApplicationAccessToken());
    var self = this;
    this.getUserAccessToken(cb.returnToCallback(callback, false, function(userAccessToken) {
      if (userAccessToken) {
        self.setAccessToken(userAccessToken);
      }
      return self.accessToken;
    }));
  }
};

BaseFacebook.prototype.getAccessToken = cb.wrap(BaseFacebook.prototype.getAccessToken);

/**
 * Determines and returns the user access token, first using
 * the signed request if present, and then falling back on
 * the authorization code if present.  The intent is to
 * return a valid user access token, or false if one is determined
 * to not be available.
 *
 * @return string A valid user access token, or false if one
 *                could not be determined.
 */
BaseFacebook.prototype.getUserAccessToken = function getUserAccessToken(callback) {
  // first, consider a signed request if it's supplied.
  // if there is a signed request, then it alone determines
  // the access token.
  var signedRequest = this.getSignedRequest();
  if (signedRequest) {
    // apps.facebook.com hands the access_token in the signed_request
    if (signedRequest.hasOwnProperty('oauth_token')) {
      var accessToken = signedRequest.oauth_token;
      this.setPersistentData('access_token', accessToken);
      callback(null, accessToken);
    }
    else {
      // the JS SDK puts a code in with the redirect_uri of ''
      if (signedRequest.hasOwnProperty('code')) {
        var code = signedRequest.code;
        var self = this;
        this.getAccessTokenFromCode(code, null, cb.returnToCallback(callback, false, handleAccessTokenFromCode));
      }
      else {
        // signed request states there's no access token, so anything
        // stored should be cleared.
        this.clearAllPersistentData();
        // respect the signed request's data, even
        // if there's an authorization code or something else
        callback(null, false);
      }
    }
  }
  else {
    var code = this.getCode();
    if (code && code !== this.getPersistentData('code')) {
      var self = this;
      this.getAccessTokenFromCode(code, null, cb.returnToCallback(callback, false, handleAccessTokenFromCode));
    }
    else {
      // as a fallback, just return whatever is in the persistent
      // store, knowing nothing explicit (signed request, authorization
      // code, etc.) was present to shadow it (or we saw a code in $_REQUEST,
      // but it's the same as what's in the persistent store)
      callback(null, this.getPersistentData('access_token'));
    }
  }
  function handleAccessTokenFromCode(accessToken) {
    if (accessToken) {
      self.setPersistentData('code', code);
      self.setPersistentData('access_token', accessToken);
      return accessToken;
    }
    else {
      // signed request states there's no access token, so anything
      // stored should be cleared.
      self.clearAllPersistentData();
      // respect the signed request's data, even
      // if there's an authorization code or something else
      return false;
    }
  }
};

BaseFacebook.prototype.getUserAccessToken = cb.wrap(BaseFacebook.prototype.getUserAccessToken);

BaseFacebook.prototype.mergeObject = function() {
  var obj = {};
  for (var i = 0; i < arguments.length; i++) {
    var arg = arguments[i];
    for (var name in arg) {
      if (arg.hasOwnProperty(name)) {
        obj[name] = arg[name];
      }
    }
  }
  return obj;
};

/**
 * Get a Login URL for use with redirects. By default, full page redirect is
 * assumed. If you are using the generated URL with a window.open() call in
 * JavaScript, you can pass in display=popup as part of the $params.
 *
 * The parameters:
 * - redirect_uri: the url to go to after a successful login
 * - scope: comma separated list of requested extended perms
 *
 * @param array $params Provide custom parameters
 * @return string The URL for the login flow
 *
 * @throws Error
 */
BaseFacebook.prototype.getLoginUrl = function(params) {
  if (!params) {
    params = {};
  }
  this.establishCSRFTokenState();
  var currentUrl = this.getCurrentUrl();

  // if 'scope' is passed as an array, convert to comma separated list
  var scopeParams = params.hasOwnProperty('scope') ? params.scope : null;
  if (scopeParams && isArray(scopeParams)) {
    params.scope = scopeParams.join(',');
  }

  return 'https://www.facebook.com/dialog/oauth?' + querystring.stringify(this.mergeObject({
    client_id: this.getAppId(),
    redirect_uri: currentUrl, // possibly overwritten
    state: this.state
  }, params));
};

/**
 * Get a Logout URL suitable for use with redirects.
 *
 * The parameters:
 * - next: the url to go to after a successful logout
 *
 * @param array $params Provide custom parameters
 * @return string The URL for the logout flow
 */
BaseFacebook.prototype.getLogoutUrl = function getLogoutUrl(/* params, callback */) {
  var args = [].slice.call(arguments);
  var callback = args.pop();
  var params = args.shift();
  if (!params) {
    params = {};
  }

  var self = this;
  this.getAccessToken(cb.returnToCallback(callback, false, function(accessToken) {
    var currentUrl = self.getCurrentUrl();
    var queryMap = self.mergeObject({ next: currentUrl, access_token: accessToken }, params);
    var query = querystring.stringify(queryMap)
    return 'https://www.facebook.com/logout.php?' + query;
  }));
};

BaseFacebook.prototype.getLogoutUrl = cb.wrap(BaseFacebook.prototype.getLogoutUrl);

/**
 * Get a login status URL to fetch the status from Facebook.
 *
 * The parameters:
 * - ok_session: the URL to go to if a session is found
 * - no_session: the URL to go to if the user is not connected
 * - no_user: the URL to go to if the user is not signed into facebook
 *
 * @param array $params Provide custom parameters
 * @return string The URL for the logout flow
 *
 * @throws Error
 */
BaseFacebook.prototype.getLoginStatusUrl = function(params) {
  var currentUrl = this.getCurrentUrl();
  return 'https://www.facebook.com/extern/login_status.php?' + querystring.stringify(this.mergeObject({
    api_key: this.getAppId(),
    no_session: currentUrl,
    no_user: currentUrl,
    ok_session: currentUrl,
    session_version: 3
  }, params));
};

BaseFacebook.prototype.state = null;

/**
 * Lays down a CSRF state token for this process.
 *
 * @return void
 */
BaseFacebook.prototype.establishCSRFTokenState = function() {
  if (this.state === null) {
    var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var buf = [];
    for (var i = 0; i < 32; i++) {
      buf.push(chars[Math.floor(chars.length * Math.random())]);
    }
    this.state = buf.join('');
    this.setPersistentData('state', this.state);
  }
};

/**
 * Returns the access token that should be used for logged out
 * users when no authorization code is available.
 *
 * @return string The application access token, useful for gathering
 *                public information about users and applications.
 */
BaseFacebook.prototype.getApplicationAccessToken = function() {
  return this.appId + '|' + this.appSecret;
}

BaseFacebook.prototype.signedRequest = null;

/**
 * Retrieve the signed request, either from a request parameter or,
 * if not present, from a cookie.
 *
 * @return string the signed request, if available, or null otherwise.
 */
BaseFacebook.prototype.getSignedRequest = function() {
  if (this.signedRequest === null) {
    var param = this.getRequestParam('signed_request');
    if (param !== null) {
      parseSignedRequestWrapped(this, param);
    }
    if (this.signedRequest === null) {
      var cookieName = this.getSignedRequestCookieName();
      var cookieValue = this.getCookie(cookieName);
      if (cookieValue !== null) {
        parseSignedRequestWrapped(this, cookieValue);
      }
    }
  }

  function parseSignedRequestWrapped(self, str) {
    try {
      self.signedRequest = self.parseSignedRequest(str);
    }
    catch (err) {
      self.signedRequest = null;
      self.errorLog('Parse error sigened_request cookie: ' + str);
    }
  }

  return this.signedRequest;
};

BaseFacebook.prototype.user = null;

/**
 * Get the UID of the connected user, or 0
 * if the Facebook user is not connected.
 *
 * @return string the UID if available.
 */
BaseFacebook.prototype.getUser = function getUser(callback) {
  if (this.user !== null) {
    // we've already determined this and cached the value.
    callback(null, this.user);
  }
  else {
    var self = this;
    this.getUserFromAvailableData(cb.returnToCallback(callback, false, function(user) {
      self.user = user;
      return self.user;
    }));
  }
};

BaseFacebook.prototype.getUser = cb.wrap(BaseFacebook.prototype.getUser);

/**
 * Determines the connected user by first examining any signed
 * requests, then considering an authorization code, and then
 * falling back to any persistent store storing the user.
 *
 * @return integer The id of the connected Facebook user,
 *                 or 0 if no such user exists.
 */
BaseFacebook.prototype.getUserFromAvailableData = function getUserFromAvailableData(callback) {
  // if a signed request is supplied, then it solely determines
  // who the user is.
  var signedRequest = this.getSignedRequest();
  if (signedRequest) {
    if (signedRequest.hasOwnProperty('user_id')) {
      var user = signedRequest.user_id;
      assert.ok(typeof user === 'string' && user.match(/^\d+$/));
      this.setPersistentData('user_id', user);
      callback(null, user);
    }
    else {
      // if the signed request didn't present a user id, then invalidate
      // all entries in any persistent store.
      this.clearAllPersistentData();
      callback(null, 0);
    }
  }
  else {
    var user = this.getPersistentData('user_id', 0);
    var persistedAccessToken = this.getPersistentData('access_token');
    // use access_token to fetch user id if we have a user access_token, or if
    // the cached access token has changed.
    var self = this;
    this.getAccessToken(function(err, accessToken) {
      try {
        if (err) {
          throw err;
        }
        if ((accessToken) &&
          // access_token is not application access_token
            (accessToken !== self.getApplicationAccessToken()) &&
            // undefined user or access_token is old
            (!user || persistedAccessToken !== accessToken)) {

          self.getUserFromAccessToken(cb.returnToCallback(callback, false, function(user) {
            if (user) {
              assert.ok(typeof user === 'string' && user.match(/^\d+$/));
              self.setPersistentData('user_id', user);
            }
            else {
              self.clearAllPersistentData();
            }
            return user;
          }));
        }
        else {
          callback(null, user);
        }
      }
      catch (err) {
        callback(err, null);
      }
    });
  }
};

BaseFacebook.prototype.getUserFromAvailableData = cb.wrap(BaseFacebook.prototype.getUserFromAvailableData);

/**
 * Make an API call.
 *
 * @return mixed The decoded response
 */
BaseFacebook.prototype.api = function api(/* polymorphic */) {
  var args = [].slice.call(arguments);
  if (args[0] && typeof args[0] === 'object') {
    var callback = args.pop();
    this.restserver(args[0], callback);
  } else {
    this.graph.apply(this, args);
  }
};

BaseFacebook.prototype.api = cb.wrap(BaseFacebook.prototype.api);

/**
 * Invoke the old restserver.php endpoint.
 *
 * @param array $params Method call object
 *
 * @return mixed The decoded response object
 */
BaseFacebook.prototype.restserver = function restserver(params, callback) {
  // generic application level parameters
  params.api_key = this.getAppId();
  params.format = 'json-strings';

  var self = this;
  var host = this.getApiHost(params['method'])
  this.oauthRequest(host, '/restserver.php', params, cb.returnToCallback(callback, false, function(response) {
    try {
      var result = JSON.parse(response);
    }
    catch (err) {
      throw new Error('Parse REST server response error: ' + err.message);
    }
    // results are returned, errors are thrown
    if (result && typeof result === 'object' && result.hasOwnProperty('error_code')) {
      throw self.createApiError(result);
    }
    else {
      if (params.method === 'auth.expireSession' || params.method === 'auth.revokeAuthorization') {
        self.destroySession();
      }
      return result;
    }
  }));
};

BaseFacebook.prototype.restserver = cb.wrap(BaseFacebook.prototype.restserver);

/**
 * Return true if this is video post.
 *
 * @param string $path The path
 * @param string $method The http method (default 'GET')
 *
 * @return boolean true if this is video post
 */
BaseFacebook.prototype.isVideoPost = function isVideoPost(path, method) {
  method = method || 'GET';
  if (method == 'POST' && path.match(/^(\/)(.+)(\/)(videos)$/)) {
    return true;
  }
  return false;
}

/**
 * Invoke the Graph API.
 *
 * @param string $path The path (required)
 * @param string $method The http method (default 'GET')
 * @param array $params The query/post data
 *
 * @return mixed The decoded response object
 */
BaseFacebook.prototype.graph = function graph(/* path, method, params, callback */) {
  var args = [].slice.call(arguments);
  var callback = args.pop();
  var path = args.shift();
  var method = 'GET';
  var params = {};
  if (args.length === 1) {
    if (typeof args[0] === 'string') {
      method = args[0];
    }
    else {
      params = args[0];
    }
  }
  else if (args.length === 2){
    method = args[0];
    params = args[1];
  }

  params.method = method; // method override as we always do a POST

  var domain = this.isVideoPost(path, method) ? 'graph-video.facebook.com' : 'graph.facebook.com';

  var self = this;
  this.oauthRequest(domain, path, params, cb.returnToCallback(callback, false, function(response) {
    try {
      result = JSON.parse(response);
    }
    catch (err) {
      throw new Error('Parse Graph API server response error: ' + err.message);
    }
    if (result && typeof result === 'object' && result.hasOwnProperty('error')) {
      throw self.createApiError(result);
    }
    else {
      return result;
    }
  }));
};

BaseFacebook.prototype.graph = cb.wrap(BaseFacebook.prototype.graph);

/**
 * Analyzes the supplied result to see if it was thrown
 * because the access token is no longer valid.  If that is
 * the case, then we destroy the session.
 *
 * @param $result array A record storing the error message returned
 *                      by a failed API call.
 */
BaseFacebook.prototype.createApiError = function(result) {
  var err = new FacebookApiError(result);
  switch (err.getType()) {
    // OAuth 2.0 Draft 00 style
    case 'OAuthException':
      // OAuth 2.0 Draft 10 style
    case 'invalid_token':
      // REST server errors are just Exceptions
    case 'Exception':
      var message = err.message;
      if ((message.indexOf('Error validating access token') !== -1) ||
          (message.indexOf('Invalid OAuth access token') !== -1) ||
          (message.indexOf('An active access token must be used') !== -1)) {
        this.destroySession();
      }
      break;
  }
  return err;
};

/**
 * Destroy the current session
 */
BaseFacebook.prototype.destroySession = function() {
  this.accessToken = null;
  this.signedRequest = null;
  this.user = null;
  this.clearAllPersistentData();

  if (this.request) {
    // Javascript sets a cookie that will be used in getSignedRequest that we
    // need to clear if we can
    var cookieName = this.getSignedRequestCookieName();

    if (this.hasCookie(cookieName)) {
      if (this.request.cookies) {
        delete this.request.cookies[cookieName];
      }

      if (this.response) {
        if (!this.sentHeaders()) {
          // The base domain is stored in the metadata cookie if not we fallback
          // to the current hostname
          var host = this.request.headers['x-forwarded-host'] || this.request.headers.host;
          var baseDomain = '.' + host;

          var metadata = this.getMetadataCookie();
          if (metadata.hasOwnProperty('base_domain') && typeof metadata['base_domain'] === 'string' && metadata['base_domain'] !== '') {
            baseDomain = metadata['base_domain'];
          }

          this.clearCookie(cookieName, { path: '/', domain: baseDomain });
        }
        else {
          this.errorLog(
            'There exists a cookie that we wanted to clear that we couldn\'t ' +
            'clear because headers was already sent. Make sure to do the first ' +
            'API call before outputing anything'
          );
        }
      }
    }
  }
};

/**
 * Parses the metadata cookie that our Javascript API set
 *
 * @return  an array mapping key to value
 */
BaseFacebook.prototype.getMetadataCookie = function getMetadataCookie() {
  var cookieName = this.getMetadataCookieName();
  if (!this.hasCookie(cookieName)) {
    return {};
  }

  // The cookie value can be wrapped in "-characters so remove them
  var cookieValue = this.getCookie(cookieName);
  cookieValue = cookieValue.replace(/"/g, '');

  if (cookieValue === '') {
    return {};
  }

  var parts = cookieValue.split(/&/);
  var metadata = {};
  for (var i = 0; i < parts.length; i++) {
    var part = parts[i];
    var pair = part.split(/=/, 2);
    if (pair[0] !== '') {
      metadata[decodeURIComponent(pair[0])] = (pair.length > 1) ? decodeURIComponent(pair[1]) : '';
    }
  }

  return metadata;
};

BaseFacebook.prototype.apiReadOnlyCalls = {
  'admin.getallocation': true,
  'admin.getappproperties': true,
  'admin.getbannedusers': true,
  'admin.getlivestreamvialink': true,
  'admin.getmetrics': true,
  'admin.getrestrictioninfo': true,
  'application.getpublicinfo': true,
  'auth.getapppublickey': true,
  'auth.getsession': true,
  'auth.getsignedpublicsessiondata': true,
  'comments.get': true,
  'connect.getunconnectedfriendscount': true,
  'dashboard.getactivity': true,
  'dashboard.getcount': true,
  'dashboard.getglobalnews': true,
  'dashboard.getnews': true,
  'dashboard.multigetcount': true,
  'dashboard.multigetnews': true,
  'data.getcookies': true,
  'events.get': true,
  'events.getmembers': true,
  'fbml.getcustomtags': true,
  'feed.getappfriendstories': true,
  'feed.getregisteredtemplatebundlebyid': true,
  'feed.getregisteredtemplatebundles': true,
  'fql.multiquery': true,
  'fql.query': true,
  'friends.arefriends': true,
  'friends.get': true,
  'friends.getappusers': true,
  'friends.getlists': true,
  'friends.getmutualfriends': true,
  'gifts.get': true,
  'groups.get': true,
  'groups.getmembers': true,
  'intl.gettranslations': true,
  'links.get': true,
  'notes.get': true,
  'notifications.get': true,
  'pages.getinfo': true,
  'pages.isadmin': true,
  'pages.isappadded': true,
  'pages.isfan': true,
  'permissions.checkavailableapiaccess': true,
  'permissions.checkgrantedapiaccess': true,
  'photos.get': true,
  'photos.getalbums': true,
  'photos.gettags': true,
  'profile.getinfo': true,
  'profile.getinfooptions': true,
  'stream.get': true,
  'stream.getcomments': true,
  'stream.getfilters': true,
  'users.getinfo': true,
  'users.getloggedinuser': true,
  'users.getstandardinfo': true,
  'users.hasapppermission': true,
  'users.isappuser': true,
  'users.isverified': true,
  'video.getuploadlimits': true
};

/**
 * Build the URL for api given parameters.
 *
 * @param $method String the method name.
 * @return string The URL for the given parameters
 */
BaseFacebook.prototype.getApiHost = function(method) {
  var host = 'api.facebook.com';
  if (this.apiReadOnlyCalls.hasOwnProperty(method.toLowerCase())) {
    host = 'api-read.facebook.com';
  }
  else if (method.toLowerCase() === 'video.upload') {
    host = 'api-video.facebook.com';
  }
  return host;
};

/**
 * Constructs and returns the name of the cookie that
 * potentially houses the signed request for the app user.
 * The cookie is not set by the BaseFacebook class, but
 * it may be set by the JavaScript SDK.
 *
 * @return string the name of the cookie that would house
 *         the signed request value.
 */
BaseFacebook.prototype.getSignedRequestCookieName = function() {
  return 'fbsr_' + this.getAppId();
};

/**
 * Parses a signed_request and validates the signature.
 *
 * @param string $signed_request A signed token
 * @return array The payload inside it or null if the sig is wrong
 *
 * @throws Error
 */
BaseFacebook.prototype.parseSignedRequest = function(signedRequest) {
  var splittedSignedRequest = signedRequest.split(/\./);
  var encodedSig = splittedSignedRequest.shift();
  var payload = splittedSignedRequest.join('.');

  // decode the data
  var sig = this.base64UrlDecode(encodedSig);

  // must catch in caller
  var data = JSON.parse(this.base64UrlDecode(payload).toString('utf8'));

  if (data.algorithm.toUpperCase() !== 'HMAC-SHA256') {
    this.errorLog('Unknown algorithm. Expected HMAC-SHA256');
    return null;
  }

  // check sig
  var hmac = crypto.createHmac('sha256', this.getAppSecret());
  hmac.update(payload);
  var expectedSig = hmac.digest('base64');
  if (sig.toString('base64') !== expectedSig) {
    this.errorLog('Bad Signed JSON signature!');
    return null;
  }

  return data;
};

BaseFacebook.prototype.base64UrlDecode = function(input) {
  var base64 = input.replace(/-/g, '+').replace(/_/g, '/');
  return new Buffer(base64, 'base64');
};

/**
 * Constructs and returns the name of the coookie that potentially contain
 * metadata. The cookie is not set by the BaseFacebook class, but it may be
 * set by the JavaScript SDK.
 *
 * @return string the name of the cookie that would house metadata.
 */
BaseFacebook.prototype.getMetadataCookieName = function getMetadataCookieName() {
  return 'fbm_' + this.getAppId();
};

/**
 * Get the authorization code from the query parameters, if it exists,
 * and otherwise return false to signal no authorization code was
 * discoverable.
 *
 * @return mixed The authorization code, or false if the authorization
 *               code could not be determined.
 */
BaseFacebook.prototype.getCode = function() {
  var code = this.getRequestParam('code');
  if (code !== null) {
    var state = this.getRequestParam('state');
    if (this.state !== null && state !== null && this.state === state) {

      // CSRF state has done its job, so clear it
      this.state = null;
      this.clearPersistentData('state');
      return code;
    } else {
      this.errorLog('CSRF state token does not match one provided.');
      return false;
    }
  }

  return false;
};

/**
 * Retrieves the UID with the understanding that
 * $this->accessToken has already been set and is
 * seemingly legitimate.  It relies on Facebook's Graph API
 * to retrieve user information and then extract
 * the user ID.
 *
 * @return integer Returns the UID of the Facebook user, or 0
 *                 if the Facebook user could not be determined.
 */
BaseFacebook.prototype.getUserFromAccessToken = function getUserFromAccessToken(callback) {
  this.api('/me', cb.returnToCallback(callback, true, function(err, userInfo) {
    if (err) {
      return 0;
    }
    else {
      return userInfo.id;
    }
  }));
};

BaseFacebook.prototype.getUserFromAccessToken = cb.wrap(BaseFacebook.prototype.getUserFromAccessToken);

/**
 * Retrieves an access token for the given authorization code
 * (previously generated from www.facebook.com on behalf of
 * a specific user).  The authorization code is sent to graph.facebook.com
 * and a legitimate access token is generated provided the access token
 * and the user for which it was generated all match, and the user is
 * either logged in to Facebook or has granted an offline access permission.
 *
 * @param string $code An authorization code.
 * @return mixed An access token exchanged for the authorization code, or
 *               false if an access token could not be generated.
 */
BaseFacebook.prototype.getAccessTokenFromCode = function getAccessTokenFromCode(code, redirectUri, callback) {
  if (!code) {
    callback(null, false);
  }
  else {
    if (!redirectUri) {
      redirectUri = this.getCurrentUrl();
    }

    // need to circumvent json_decode by calling oauthRequest
    // directly, since response isn't JSON format.
    this.oauthRequest('graph.facebook.com', '/oauth/access_token', {
      client_id: this.getAppId(),
      client_secret: this.getAppSecret(),
      redirect_uri: redirectUri,
      code: code
    }, 
    cb.returnToCallback(callback, true, function(err, accessTokenResponse) {
      if (err) {
        if (err instanceof FacebookApiError) {
          // most likely that user very recently revoked authorization.
          // In any event, we don't have an access token, so say so.
          return false;
        }
        else {
          throw err;
        }
      }
      else {
        if (!accessTokenResponse) {
          return false;
        }
        else {
          var responseParams = querystring.parse(accessTokenResponse);
          if (!responseParams.hasOwnProperty('access_token')) {
            return false;
          }
          else {
            return responseParams.access_token;
          }
        }
      }
    }));
  }
};

BaseFacebook.prototype.getAccessTokenFromCode = cb.wrap(BaseFacebook.prototype.getAccessTokenFromCode);

/**
 * Returns the Current URL, stripping it of known FB parameters that should
 * not persist.
 *
 * @return string The current URL
 * @throws Errror
 */
BaseFacebook.prototype.getCurrentUrl = function() {
  if (this.currentUrl !== null) {
    return this.currentUrl;
  }

  if (!this.request) {
    throw new Error('No request object.');
  }

  var req = this.request;
  var conn = req.connection;
  var headers = req.headers;
  if (conn.pair || req.https === 'on' || headers['x-forwarded-proto'] === 'https') {
    var protocol = 'https://';
  }
  else {
    var protocol = 'http://';
  }

  var host = headers['x-forwarded-host'] || headers.host;
  var path = req.url;

  var currentUrl = protocol + host + path;

  var parts = url.parse(currentUrl);

  if (parts.query) {
    var params = parts.query.split(/&/);
    var self = this;
    delete parts.href;
    delete parts.path;
    delete parts.query;

    parts.search = '';

    params = params.filter(function(param) { return self.shouldRetainParam(param) });
    if (params.length > 0) {
      parts.search = '?' + params.join('&');
    }
  }

  return url.format(parts);
};

/**
 * Returns true if and only if the key or key/value pair should
 * be retained as part of the query string.  This amounts to
 * a brute-force search of the very small list of Facebook-specific
 * params that should be stripped out.
 *
 * @param string $param A key or key/value pair within a URL's query (e.g.
 *                     'foo=a', 'foo=', or 'foo'.
 *
 * @return boolean
 */
BaseFacebook.prototype.shouldRetainParam = function(param) {
  var splited = param.split(/=/);
  return !((splited.length > 1) && this.dropQueryParams.hasOwnProperty(splited[0]));
};

BaseFacebook.prototype.dropQueryParams = {
  code: true,
  state: true,
  signed_request: true,
  base_domain: true
};

/**
 * Make a OAuth Request.
 *
 * @param string $url The path (required)
 * @param array $params The query/post data
 *
 * @return string The decoded response object
 */
BaseFacebook.prototype.oauthRequest = function oauthRequest(host, path, params, callback) {
  var self = this;
  if (!params.hasOwnProperty('access_token')) {
    this.getAccessToken(function(err, accessToken) {
      try {
        if (err) {
          throw err;
        }
        params['access_token'] = accessToken;
        next();
      }
      catch (err) {
        callback(err, null);
      }
    });
  }
  else {
    next();
  }
  function next() {
    // json_encode all params values that are not strings
    for (var key in params) {
      var value = params[key];
      if (typeof value !== 'string') {
        params[key] = JSON.stringify(value);
      }
    }

    self.makeRequest(host, path, params, callback);
  }
};

BaseFacebook.prototype.oauthRequest = cb.wrap(BaseFacebook.prototype.oauthRequest);

/**
 * Makes an HTTP request. This method can be overridden by subclasses if
 * developers want to do fancier things or use something other than curl to
 * make the request.
 *
 * @param string host The Host to make the request to
 * @param string path The URL to make the request to
 * @param array params The parameters to use for the POST body
 * @param callback
 */
BaseFacebook.prototype.makeRequest = function makeRequest(host, path, params, callback) {
  requestUtil.requestFacebookApi(https, host, 443, path, params, this.fileUploadSupport, callback);
};

BaseFacebook.prototype.makeRequest = cb.wrap(BaseFacebook.prototype.makeRequest);

/**
 * Prints to the error log if you aren't in command line mode.
 *
 * @param string $msg Log message
 */
BaseFacebook.prototype.errorLog = function(msg) {
  util.debug(msg);
};

/**
 * Thrown when an API call returns an exception.
 *
 * @author Naitik Shah <naitik@facebook.com>
 */
function FacebookApiError(result) {
  this.result = result;

  this.code = this.result.hasOwnProperty('error_code') ? result.error_code : 0;

  if (result.hasOwnProperty('error_description')) {
    // OAuth 2.0 Draft 10 style
    var msg = result.error_description;
  } else if (result.hasOwnProperty('error') && result.error && typeof (result.error) === 'object') {
    // OAuth 2.0 Draft 00 style
    var msg = result.error.message;
  } else if (result.hasOwnProperty('error_msg')) {
    // Rest server style
    var msg = result.error_msg;
  } else {
    var msg = 'Unknown Error. Check getResult()';
  }

  Error.apply(this, []);
  this.message = msg;
}

util.inherits(FacebookApiError, Error);

/**
 * The result from the API server that represents the exception information.
 */
FacebookApiError.prototype.result = null;

/**
 * Return the associated result object returned by the API server.
 *
 * @return array The result from the API server
 */
FacebookApiError.prototype.getResult = function() {
  return this.result;
};

/**
 * Returns the associated type for the error. This will default to
 * 'Error' when a type is not available.
 *
 * @return string
 */
FacebookApiError.prototype.getType = function() {
  if (this.result.hasOwnProperty('error')) {
    var error = this.result.error;
    if (typeof error === 'string') {
      // OAuth 2.0 Draft 10 style
      return error;
    }
    else if (error && typeof error === 'object') {
      // OAuth 2.0 Draft 00 style
      if (error.hasOwnProperty('type')) {
        return error.type;
      }
    }
  }

  return 'Error';
};

/**
 * To make debugging easier.
 *
 * @return string The string representation of the error
 */
FacebookApiError.prototype.toString = function() {
  var str = this.getType() + ': ';
  if (this.code !== 0) {
    str += this.code + ': ';
  }
  return str + this.message;
};

// for test
BaseFacebook.FacebookApiError = FacebookApiError;

function isArray(ar) {
  return Array.isArray(ar) || (typeof ar === 'object' && Object.prototype.toString.call(ar) === '[object Array]');
}

module.exports = BaseFacebook;
