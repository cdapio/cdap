var path = require('path');
var util = require('util');
var url = require('url');
var testUtil = require('./lib/testutil.js');

var BaseFacebook = require(path.join(testUtil.libdir, 'basefacebook.js'));

var config = testUtil.fbDefaultConfig;

module.exports = {

  constructor: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });
    assert.equal(facebook.getAppId(), config.appId, 'Expect the App ID to be set.');
    assert.equal(facebook.getAppSecret(), config.secret, 'Expect the app secret to be set.');
    // for compatibility
    assert.equal(facebook.getApiSecret(), config.secret, 'Expect the app secret to be set.');
    assert.equal(facebook.getApplicationAccessToken(), config.appId + '|' + config.secret);
    done = true;
  },

  constructorWithFileUpload: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      fileUpload: true
    });
    assert.equal(facebook.getAppId(), config.appId, 'Expect the App ID to be set.');
    assert.equal(facebook.getAppSecret(), config.secret, 'Expect the app secret to be set.');
    // for compatibility
    assert.equal(facebook.getApiSecret(), config.secret, 'Expect the app secret to be set.');
    assert.ok(facebook.getFileUploadSupport(), 'Expect file upload support to be on.');
    // alias (depricated) for getFileUploadSupport -- test until removed
    assert.ok(facebook.useFileUploadSupport(), 'Expect file upload support to be on.');
    done = true;
  },

  setAppId: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });
    facebook.setAppId('dummy');
    assert.equal(facebook.getAppId(), 'dummy', 'Expect the App ID to be dummy.');
    done = true;
  },

  // for compatibility
  setApiSecret: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });
    facebook.setApiSecret('dummy');
    assert.equal(facebook.getApiSecret(), 'dummy', 'Expect the app secret to be dummy.');
    done = true;
  },

  setAppSecret: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });
    facebook.setAppSecret('dummy');
    assert.equal(facebook.getAppSecret(), 'dummy', 'Expect the app secret to be dummy.');
    done = true;
  },

  setAccessToken: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.setAccessToken('saltydog');
    facebook.getAccessToken(function(err, accessToken) {
      assert.equal(err, null);
      assert.equal(accessToken, 'saltydog',
                        'Expect installed access token to remain \'saltydog\'');
      done = true;
    });
  },

  setFileUploadSupport: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });
    assert.equal(facebook.getFileUploadSupport(), false, 'Expect file upload support to be off.');
    // alias for getFileUploadSupport (depricated), testing until removed
    assert.equal(facebook.useFileUploadSupport(), false, 'Expect file upload support to be off.');
    facebook.setFileUploadSupport(true);
    assert.ok(facebook.getFileUploadSupport(), 'Expect file upload support to be on.');
    // alias for getFileUploadSupport (depricated), testing until removed
    assert.ok(facebook.useFileUploadSupport(), 'Expect file upload support to be on.');
    done = true;
  },

  getCurrentUrl: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php?one=one&two=two&three=three'
      }
    });

    var currentUrl = facebook.getCurrentUrl();
    assert.equal('http://www.test.com/unit-tests.php?one=one&two=two&three=three',
                  currentUrl, 'getCurrentUrl function is changing the current URL');

    facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php?one=&two=&three='
      }
    });

    currentUrl = facebook.getCurrentUrl();
    assert.equal('http://www.test.com/unit-tests.php?one=&two=&three=',
                  currentUrl, 'getCurrentUrl function is changing the current URL');

    facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php?one&two&three'
      }
    });

    currentUrl = facebook.getCurrentUrl();
    assert.equal('http://www.test.com/unit-tests.php?one&two&three',
                  currentUrl, 'getCurrentUrl function is changing the current URL');

    facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php?one&two&three&state=hoge'
      }
    });

    currentUrl = facebook.getCurrentUrl();
    assert.equal('http://www.test.com/unit-tests.php?one&two&three',
                  currentUrl, 'getCurrentUrl function is changing the current URL');

    facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php?state=hoge'
      }
    });

    currentUrl = facebook.getCurrentUrl();
    assert.equal('http://www.test.com/unit-tests.php', currentUrl);

    facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php?state=hoge&base_domain=test.com'
      }
    });

    currentUrl = facebook.getCurrentUrl();
    assert.equal('http://www.test.com/unit-tests.php', currentUrl);

    facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      currentUrl: 'http://example.com/',
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php?state=hoge'
      }
    });

    assert.equal(facebook.getCurrentUrl(), 'http://example.com/');

    done = true;
  },

  getLoginUrl: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php'
      }
    });

    var loginUrl = url.parse(facebook.getLoginUrl(), true);
    assert.equal(loginUrl.protocol, 'https:');
    assert.equal(loginUrl.host, 'www.facebook.com');
    assert.equal(loginUrl.pathname, '/dialog/oauth');
    assert.equal(loginUrl.query.client_id, config.appId);
    assert.equal(loginUrl.query.redirect_uri, 'http://www.test.com/unit-tests.php');
    assert.equal(loginUrl.query.state.length, 32);
    done = true;
  },

  getLoginURLWithExtraParams: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php'
      }
    });

    var extraParams = {
      scope: 'email, sms',
      nonsense: 'nonsense'
    };
    var loginUrl = url.parse(facebook.getLoginUrl(extraParams), true);
    assert.equal(loginUrl.protocol, 'https:');
    assert.equal(loginUrl.host, 'www.facebook.com');
    assert.equal(loginUrl.pathname, '/dialog/oauth');
    assert.equal(loginUrl.query.client_id, config.appId);
    assert.equal(loginUrl.query.redirect_uri, 'http://www.test.com/unit-tests.php');
    assert.equal(loginUrl.query.scope, extraParams.scope);
    assert.equal(loginUrl.query.nonsense, extraParams.nonsense);
    assert.equal(loginUrl.query.state.length, 32);
    done = true;
  },

  getLoginURLWithScopeParamsAsArray: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'www.test.com'
        },
        url: '/unit-tests.php'
      }
    });

    var scopeParamsAsArray = ['email','sms','read_stream'];
    var extraParams = {
      scope: scopeParamsAsArray,
      nonsense: 'nonsense'
    };
    var loginUrl = url.parse(facebook.getLoginUrl(extraParams), true);
    assert.equal(loginUrl.protocol, 'https:');
    assert.equal(loginUrl.host, 'www.facebook.com');
    assert.equal(loginUrl.pathname, '/dialog/oauth');
    assert.equal(loginUrl.query.client_id, config.appId);
    assert.equal(loginUrl.query.redirect_uri, 'http://www.test.com/unit-tests.php');
    assert.equal(loginUrl.query.scope, scopeParamsAsArray.join(','));
    assert.equal(loginUrl.query.nonsense, extraParams.nonsense);
    assert.equal(loginUrl.query.state.length, 32);
    done = true;
  },

  getCodeWithValidCSRFState: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        query: {}
      }
    });

    facebook.establishCSRFTokenState();

    var code = facebook.request.query.code = 'dummy';
    facebook.request.query.state = facebook.getPersistentData('state');
    assert.equal(code, facebook.getCode(), 'Expect code to be pulled from $_REQUEST[\'code\']');
    done = true;
  },

  getCodeWithInvalidCSRFState: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        query: {}
      }
    });

    facebook.establishCSRFTokenState();

    var code = facebook.request.query.code = 'dummy';
    facebook.request.query.state = facebook.getPersistentData('state') + 'forgery!!!';
    facebook.errorLog = function() {};
    assert.equal(facebook.getCode(), false, 'Expect getCode to fail, CSRF state should not match.');
    done = true;
  },

  getCodeWithMissingCSRFState: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        query: {}
      }
    });

    var code = facebook.request.query.code = 'dummy';
    // intentionally don't set CSRF token at all
    facebook.errorLog = function() {};
    assert.equal(facebook.getCode(), false, 'Expect getCode to fail, CSRF state not sent back.');
    done = true;
  },

  getUserFromSignedRequest: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204',
      request: {
        body: {
          signed_request: '1sxR88U4SW9m6QnSxwCEw_CObqsllXhnpP5j2pxD97c.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImV4cGlyZXMiOjEyODEwNTI4MDAsIm9hdXRoX3Rva2VuIjoiMTE3NzQzOTcxNjA4MTIwfDIuVlNUUWpub3hYVVNYd1RzcDB1U2g5d19fLjg2NDAwLjEyODEwNTI4MDAtMTY3Nzg0NjM4NXx4NURORHBtcy1nMUM0dUJHQVYzSVdRX2pYV0kuIiwidXNlcl9pZCI6IjE2Nzc4NDYzODUifQ'
        }
      }
    });

    facebook.getUser(function(err, userId) {
      assert.equal(err, null);
      assert.equal('1677846385', userId, 'Failed to get user ID from a valid signed request.');
      done = true;
    });
  },

  getSignedRequestFromCookie: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204',
      request: {
        cookies: {
        }
      }
    });

    facebook.request.cookies[facebook.getSignedRequestCookieName()] = '1sxR88U4SW9m6QnSxwCEw_CObqsllXhnpP5j2pxD97c.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImV4cGlyZXMiOjEyODEwNTI4MDAsIm9hdXRoX3Rva2VuIjoiMTE3NzQzOTcxNjA4MTIwfDIuVlNUUWpub3hYVVNYd1RzcDB1U2g5d19fLjg2NDAwLjEyODEwNTI4MDAtMTY3Nzg0NjM4NXx4NURORHBtcy1nMUM0dUJHQVYzSVdRX2pYV0kuIiwidXNlcl9pZCI6IjE2Nzc4NDYzODUifQ';
    assert.notEqual(facebook.getSignedRequest(), null);
    facebook.getUser(function(err, userId) {
      assert.equal(err, null);
      assert.equal('1677846385', userId, 'Failed to get user ID from a valid signed request.');
      done = true;
    });
  },

  getSignedRequestWithIncorrectSignature: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204',
      request: {
        cookies: {
        }
      }
    });

    facebook.request.cookies[facebook.getSignedRequestCookieName()] = '1sxR32U4SW9m6QnSxwCEw_CObqsllXhnpP5j2pxD97c.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImV4cGlyZXMiOjEyODEwNTI4MDAsIm9hdXRoX3Rva2VuIjoiMTE3NzQzOTcxNjA4MTIwfDIuVlNUUWpub3hYVVNYd1RzcDB1U2g5d19fLjg2NDAwLjEyODEwNTI4MDAtMTY3Nzg0NjM4NXx4NURORHBtcy1nMUM0dUJHQVYzSVdRX2pYV0kuIiwidXNlcl9pZCI6IjE2Nzc4NDYzODUifQ';
    facebook.errorLog = function() { };
    assert.equal(facebook.getSignedRequest(), null);
    facebook.getUser(function(err, userId) {
      assert.equal(err, null);
      assert.equal(0, userId, 'Failed to get user ID from a valid signed request.');
      done = true;
    });
  },

  nonUserAccessToken: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    // no cookies, and no request params, so no user or code,
    // so no user access token (even with cookie support)
    facebook.getAccessToken(function(err, accessToken) {
      assert.equal(err, null);
      assert.equal(facebook.getApplicationAccessToken(), accessToken,
                'Access token should be that for logged out users.');
      done = true;
    });
  },

  getAccessTokenFromCode: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {},
        headers: {}
      }
    });
    facebook.getAccessTokenFromCode('dummy', '', function(err, response) {
      assert.equal(err, null);
      assert.equal(response, false);
      facebook.getAccessTokenFromCode(null, '', function() {
        assert.equal(err, null);
        assert.equal(response, false);

        facebook.oauthRequest = function(host, path, params, callback) {
          assert.equal(host, 'graph.facebook.com');
          assert.equal(path, '/oauth/access_token');
          assert.equal(params.client_id, config.appId);
          assert.equal(params.client_secret, config.secret);
          assert.equal(params.redirect_uri, 'http://example.com/');
          assert.equal(params.code, 'dummy');
          callback(new Error('test'), null);
        };
        facebook.getAccessTokenFromCode('dummy', 'http://example.com/', function(err, response) {
          assert.ok(err instanceof Error);
          assert.equal(err.message, 'test');
          assert.equal(response, null);

          facebook.oauthRequest = function(host, path, params, callback) {
            callback(new BaseFacebook.FacebookApiError({}), null);
          };
          facebook.getAccessTokenFromCode('dummy', 'http://example.com/', function(err, response) {
            assert.equal(err, null);
            assert.equal(response, false);

            facebook.oauthRequest = function(host, path, params, callback) {
              callback(null, {});
            };
            facebook.getAccessTokenFromCode('dummy', 'http://example.com/', function(err, response) {
              assert.equal(err, null);
              assert.equal(response, false);

              facebook.oauthRequest = function(host, path, params, callback) {
                callback(null, { access_token: 'test_access_token' });
              };
              facebook.getAccessTokenFromCode('dummy', 'http://example.com/', function(err, response) {
                assert.equal(err, null);
                assert.equal(response, false);

                facebook.oauthRequest = function(host, path, params, callback) {
                  callback(null, 'access_token=dummy-access-token&expires=3600');
                };
                facebook.getAccessTokenFromCode('dummy', 'http://example.com/', function(err, response) {
                  assert.equal(err, null);
                  assert.equal(response, 'dummy-access-token');
                  done = true;
                });
              });
            });
          });
        });
      });
    });
  },

  apiForLoggedOutUsers: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.api({ method: 'fql.query', query: 'SELECT name FROM user WHERE uid=4' }, function(err, response) {
      assert.equal(err, null);
      assert.ok(isArray(response));
      assert.equal(response.length, 1, 'Expect one row back.');
      assert.equal(response[0].name, 'Mark Zuckerberg', 'Expect the name back.');
      done = true;
    });
  },

  apiWithBogusAccessToken: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.setAccessToken('this-is-not-really-an-access-token');
    // if we don't set an access token and there's no way to
    // get one, then the FQL query below works beautifully, handing
    // over Zuck's public data.  But if you specify a bogus access
    // token as I have right here, then the FQL query should fail.
    // We could return just Zuck's public data, but that wouldn't
    // advertise the issue that the access token is at worst broken
    // and at best expired.
    facebook.api({ method: 'fql.query', query: 'SELECT name FROM profile WHERE id=4' }, function(err, response) {
      assert.notEqual(null, err);
      var result = err.getResult();
      assert.ok(result && typeof result === 'object', 'expect a result object');
      assert.equal('190', result.error_code, 'expect code')
      done = true;
    });
  },

  apiGraphPublicData: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.api('/jerry', function(err, response) {
      assert.equal(err, null);
      assert.equal(response.id, '214707', 'should get expected id.');
      done = true;
    });
  },

  graphAPIWithBogusAccessToken: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.setAccessToken('this-is-not-really-an-access-token');
    facebook.api('/me', function(err, response) {
      assert.equal(response, null);
      assert.notEqual(err, null);
      assert.equal(err + '', 'OAuthException: Invalid OAuth access token.', 'Expect the invalid OAuth token message.');
      done = true;
    });
  },

/*
  // TODO Create test user and application pattern.
  graphAPIWithExpiredAccessToken: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.setAccessToken('TODO');
    facebook.api('/me', function(err, response) {
      assert.equal(response, null);
      assert.notEqual(err, null);
      assert.equal(err + '', 'OAuthException: Error validating access token:', 'Expect the invalid OAuth token message.');
      done = true;
    });
  },

  graphAPIMethod: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.api('/amachang', 'DELETE', function(err, response) {
      console.log(err, response);
      assert.equal(response, null);
      assert.notEqual(err, null);
      assert.equal(err + '',
        'OAuthException: A user access token is required to request this resource.',
        'Expect the invalid session message.');
      done = true;
    });
  },

  graphAPIOAuthSpecError: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.migratedAppId,
      secret: config.migratedSecret
    });

    facebook.api('/me', { client_id: config.migratedAppId }, function(err, response) {
      assert.equal(response, null);
      assert.notEqual(err, null);
      assert.equal(err + '',
        'invalid_request: An active access token must be used to query information about the current user.',
        'Expect the invalid session message.');
      done = true;
    });
  },

  graphAPIMethodOAuthSpecError: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.migratedAppId,
      secret: config.migratedSecret
    });

    facebook.api('/daaku.shah', 'DELETE', { client_id: config.migratedAppId }, function(err, response) {
      assert.equal(response, null);
      assert.notEqual(err, null);
      assert.equal((err + '').indexOf('invalid_request'), 0);
      done = true;
    });
  },
*/

  graphAPIWithOnlyParams: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });
    facebook.api('/jerry', function(err, response) {
      assert.equal(err, null);
      assert.notEqual(response, null);
      assert.ok(response.hasOwnProperty('id'), 'User ID should be public.');
      assert.ok(response.hasOwnProperty('name'), 'User\'s name should be public.');
      assert.ok(response.hasOwnProperty('first_name'), 'User\'s first name should be public.');
      assert.ok(response.hasOwnProperty('last_name'), 'User\'s last name should be public.');
      assert.equal(response.hasOwnProperty('work'), false,
                         'User\'s work history should only be available with ' +
                         'a valid access token.');
      assert.equal(response.hasOwnProperty('education'), false,
                         'User\'s education history should only be ' +
                         'available with a valid access token.');
      assert.equal(response.hasOwnProperty('verified'), false,
                         'User\'s verification status should only be ' +
                         'available with a valid access token.');
      done = true;
    });
  },

  loginURLDefaults: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'fbrell.com'
        },
        url: '/examples'
      }
    });
    var encodedUrl = encodeURIComponent('http://fbrell.com/examples');
    assert.notEqual(facebook.getLoginUrl().indexOf(encodedUrl), -1,
                         'Expect the current url to exist.');
    done = true;
  },

  loginURLDefaultsDropStateQueryParam: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'fbrell.com'
        },
        url: '/examples?state=xx42xx'
      }
    });

    var expectEncodedUrl = encodeURIComponent('http://fbrell.com/examples');
    assert.notEqual(facebook.getLoginUrl().indexOf(expectEncodedUrl), -1,
                      'Expect the current url to exist.');
    assert.equal(facebook.getLoginUrl().indexOf('xx42xx'), -1, 'Expect the session param to be dropped.');
    done = true;
  },

  loginURLDefaultsDropCodeQueryParam: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'fbrell.com'
        },
        url: '/examples?code=xx42xx'
      }
    });

    var expectEncodedUrl = encodeURIComponent('http://fbrell.com/examples');
    assert.notEqual(facebook.getLoginUrl().indexOf(expectEncodedUrl), -1, 'Expect the current url to exist.');
    assert.equal(facebook.getLoginUrl().indexOf('xx42xx'), -1, 'Expect the session param to be dropped.');
    done = true;
  },

  loginURLDefaultsDropSignedRequestParamButNotOthers: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'fbrell.com'
        },
        url: '/examples?signed_request=xx42xx&do_not_drop=xx43xx'
      }
    });

    var expectEncodedUrl = encodeURIComponent('http://fbrell.com/examples');
    assert.equal(facebook.getLoginUrl().indexOf('xx42xx'), -1, 'Expect the session param to be dropped.');
    assert.notEqual(facebook.getLoginUrl().indexOf('xx43xx'), -1, 'Expect the do_not_drop param to exist.');
    done = true;
  },

  testLoginURLCustomNext: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'fbrell.com'
        },
        url: '/examples'
      }
    });

    var next = 'http://fbrell.com/custom';
    var loginUrl = facebook.getLoginUrl({
      redirect_uri: next,
      cancel_url: next
    });

    var currentEncodedUrl = encodeURIComponent('http://fbrell.com/examples');
    var expectedEncodedUrl = encodeURIComponent(next);
    assert.notEqual(loginUrl.indexOf(expectedEncodedUrl), -1);
    assert.equal(loginUrl.indexOf(currentEncodedUrl), -1);
    done = true;
  },

  logoutURLDefaults: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'fbrell.com'
        },
        url: '/examples'
      }
    });

    var encodedUrl = encodeURIComponent('http://fbrell.com/examples');
    facebook.getLogoutUrl(function(err, url) {
      assert.equal(err, null);
      assert.notEqual(url.indexOf(encodedUrl), -1, 'Expect the current url to exist.');

      var facebook = new TransientFacebook({
        appId: 'dummy',
        secret: 'dummy'
      });
      facebook.getUserAccessToken = function(callback) {
        callback(new Error('dummy-error'), null);
      };
      facebook.getLogoutUrl(function(err, url) {
        assert.notEqual(err, null);
        assert.equal(err.message, 'dummy-error');
        assert.equal(url, null);

        var facebook = new TransientFacebook({
          appId: 'dummy',
          secret: 'dummy'
        });
        facebook.getLogoutUrl(function(err, url) {
          assert.notEqual(err, null);
          assert.equal(err.message, 'No request object.');
          assert.equal(url, null);
          done = true;
        });
      });
    });
  },

  loginStatusURLDefaults: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'fbrell.com'
        },
        url: '/examples'
      }
    });

    var encodedUrl = encodeURIComponent('http://fbrell.com/examples');
    assert.notEqual(facebook.getLoginStatusUrl().indexOf(encodedUrl), -1,
                         'Expect the current url to exist.');
    done = true;
  },

  loginStatusURLCustom: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'fbrell.com'
        },
        url: '/examples'
      }
    });

    var encodedUrl1 = encodeURIComponent('http://fbrell.com/examples');
    var okUrl = 'http://fbrell.com/here1';
    var encodedUrl2 = encodeURIComponent(okUrl);
    var loginStatusUrl = facebook.getLoginStatusUrl({
      ok_session: okUrl
    });
    assert.notEqual(loginStatusUrl.indexOf(encodedUrl1), -1, 'Expect the current url to exist.');
    assert.notEqual(loginStatusUrl.indexOf(encodedUrl2), -1, 'Expect the current url to exist.');
    done = true;
  },

  nonDefaultPort: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
        },
        headers: {
          host: 'fbrell.com:8080'
        },
        url: '/examples'
      }
    });

    var encodedUrl = encodeURIComponent('http://fbrell.com:8080/examples');
    assert.notEqual(facebook.getLoginUrl().indexOf(encodedUrl), -1, 'Expect the current url to exist.');
    done = true;
  },

  secureCurrentUrl: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
          pair: {}
        },
        headers: {
          host: 'fbrell.com'
        },
        url: '/examples'
      }
    });
    var encodedUrl = encodeURIComponent('https://fbrell.com/examples');
    assert.notEqual(facebook.getLoginUrl().indexOf(encodedUrl), -1, 'Expect the current url to exist.');
    done = true;
  },

  secureCurrentUrlWithNonDefaultPort: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret,
      request: {
        connection: {
          pair: {}
        },
        headers: {
          host: 'fbrell.com:8080'
        },
        url: '/examples'
      }
    });
    var encodedUrl = encodeURIComponent('https://fbrell.com:8080/examples');
    assert.notEqual(facebook.getLoginUrl().indexOf(encodedUrl), -1, 'Expect the current url to exist.');
    done = true;
  },

/*
  appSecretCall: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    var properExceptionThrown = false;
    var self = this;
    facebook.api('/' + config.appId + '/insights', function(err, response) {
      assert.notEqual(err, null);
      assert.equal(response, null);
      assert.notEqual(err.message.indexOf('Requires session when calling from a desktop app'), -1,
                        'Incorrect exception type thrown when trying to gain ' +
                        'insights for desktop app without a user access token.');
      done = true;
    });
  },
*/

  base64UrlEncode: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });
    var input = 'Facebook rocks';
    var output = 'RmFjZWJvb2sgcm9ja3M';

    assert.equal(facebook.base64UrlDecode(output).toString('utf-8'), input);
    done = true;
  },

  signedToken: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204'
    });
    var payload = facebook.parseSignedRequest('1sxR88U4SW9m6QnSxwCEw_CObqsllXhnpP5j2pxD97c.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImV4cGlyZXMiOjEyODEwNTI4MDAsIm9hdXRoX3Rva2VuIjoiMTE3NzQzOTcxNjA4MTIwfDIuVlNUUWpub3hYVVNYd1RzcDB1U2g5d19fLjg2NDAwLjEyODEwNTI4MDAtMTY3Nzg0NjM4NXx4NURORHBtcy1nMUM0dUJHQVYzSVdRX2pYV0kuIiwidXNlcl9pZCI6IjE2Nzc4NDYzODUifQ');
    assert.notEqual(payload, null, 'Expected token to parse');
    assert.equal(facebook.getSignedRequest(), null);

    facebook.request = {
      body: {
        signed_request: '1sxR88U4SW9m6QnSxwCEw_CObqsllXhnpP5j2pxD97c.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImV4cGlyZXMiOjEyODEwNTI4MDAsIm9hdXRoX3Rva2VuIjoiMTE3NzQzOTcxNjA4MTIwfDIuVlNUUWpub3hYVVNYd1RzcDB1U2g5d19fLjg2NDAwLjEyODEwNTI4MDAtMTY3Nzg0NjM4NXx4NURORHBtcy1nMUM0dUJHQVYzSVdRX2pYV0kuIiwidXNlcl9pZCI6IjE2Nzc4NDYzODUifQ'
      }
    };
    assert.deepEqual(facebook.getSignedRequest(), payload);

    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204',
      request: {
        body: {
          signed_request: 'dummy'
        }
      }
    });
    facebook.errorLog = function() {};
    assert.equal(facebook.getSignedRequest(), null);

    done = true;
  },

  nonTossedSignedToken: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204'
    });
    var payload = facebook.parseSignedRequest('c0Ih6vYvauDwncv0n0pndr0hP0mvZaJPQDPt6Z43O0k.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiJ9');
    assert.notEqual(payload, null, 'Expected token to parse');
    assert.equal(facebook.getSignedRequest(), null);

    facebook.request = {
      body: {
        signed_request: 'c0Ih6vYvauDwncv0n0pndr0hP0mvZaJPQDPt6Z43O0k.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiJ9'
      }
    };
    assert.deepEqual(facebook.getSignedRequest(), { algorithm: 'HMAC-SHA256' });
    done = true;
  },

  nonGeneralSignedToken: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: 'secret-dummy'
    });
    var data = facebook.parseSignedRequest('2mYrTJ6TkHRZ1iLlFFt3He30-e5cSgvN5U9COCqoPvE.eyAiaG9nZSI6ICJmdWdhIiwgImFsZ29yaXRobSI6ICJITUFDLVNIQTI1NiIgfQ');
    assert.equal(data.hoge, 'fuga');
    assert.equal(data.algorithm, 'HMAC-SHA256');
    done = true;
  },

/*
  public function testBundledCACert() {
    $facebook = new TransientFacebook(array(
      'appId'  => self::APP_ID,
      'secret' => self::SECRET
    ));

      // use the bundled cert from the start
    Facebook::$CURL_OPTS[CURLOPT_CAINFO] =
      dirname(__FILE__) . '/../src/fb_ca_chain_bundle.crt';
    $response = $facebook->api('/naitik');

    unset(Facebook::$CURL_OPTS[CURLOPT_CAINFO]);
    $this->assertEquals(
      $response['id'], '5526183', 'should get expected id.');
  }
*/

  videoUpload: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    var host = null;
    facebook.makeRequest = function(_host, path, params, callback) {
      host = _host;
      callback(null, null);
    };
    facebook.api({ method: 'video.upload' }, function(err, response) {
      assert.equal(host, 'api-video.facebook.com', 'video.upload should go against api-video');
      done = true;
    });
  },

  getUserAndAccessTokenFromSession: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.setPersistentData('access_token', 'foobar');
    facebook.setPersistentData('user_id', '12345');
    facebook.getAccessToken(function(err, accessToken) {
      assert.equal('foobar', accessToken, 'Get access token from persistent store.');
      facebook.getUser(function(err, user) {
        assert.equal('12345', user, 'Get user id from persistent store.');
        facebook.getUser(function(err, user) {
          assert.equal('12345', user, 'Get user again');
          done = true;
        });
      });
    });
  },

  getUserAndAccessTokenFromSignedRequestNotSession: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204',
      request: {
        query: {
          signed_request: '1sxR88U4SW9m6QnSxwCEw_CObqsllXhnpP5j2pxD97c.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImV4cGlyZXMiOjEyODEwNTI4MDAsIm9hdXRoX3Rva2VuIjoiMTE3NzQzOTcxNjA4MTIwfDIuVlNUUWpub3hYVVNYd1RzcDB1U2g5d19fLjg2NDAwLjEyODEwNTI4MDAtMTY3Nzg0NjM4NXx4NURORHBtcy1nMUM0dUJHQVYzSVdRX2pYV0kuIiwidXNlcl9pZCI6IjE2Nzc4NDYzODUifQ'
        }
      }
    });

    facebook.setPersistentData('user_id', '41572');
    facebook.setPersistentData('access_token', 'dummy');
    facebook.getUser(function(err, user) {
      assert.equal(err, null);
      assert.notEqual('41572', user, 'Got user from session instead of signed request.');
      assert.equal('1677846385', user, 'Failed to get correct user ID from signed request.');
      facebook.getAccessToken(function(err, accessToken) {
        assert.equal(err, null);
        assert.notEqual(accessToken, 'dummy', 
          'Got access token from session instead of signed request.');
        assert.notEqual(accessToken.length, 0,
          'Failed to extract an access token from the signed request.');
        done = true;
      });
    });
  },

  getUserWithoutCodeOrSignedRequestOrSession: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.getUser(function(err, user) {
      assert.equal(user, 0);
      done = true;
    });
  },

  getUserWithAvailableDataError: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.getUserFromAvailableData = function(callback) {
      callback(new Error('dummy'), null);
    };
    facebook.getUser(function(err, user) {
      assert.notEqual(err, null);
      assert.equal(err.message, 'dummy');
      assert.equal(user, null);
      done = true;
    });
  },

  getAccessTokenWithUserAccessTokenError: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: 'dummy',
      secret: 'secret-dummy'
    });
    facebook.getUserAccessToken = function(callback) {
      callback(new Error('dummy-error'), null);
    };
    facebook.getAccessToken(function(err, accessToken) {
      assert.notEqual(err, null);
      assert.equal(err.message, 'dummy-error');
      assert.equal(accessToken, null);
      done = true;
    });
  },

  getUserAccessToken: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: 'dummy',
      secret: 'secret-dummy',
      request: {
        connection: {},
        headers: {},
        body: {
          signed_request: '0GCZT4MghxPvJ7dDH84rLxeNp01h5FstqDVKuBHHkH8.eyAiY29kZSI6ICJkdW1teSIsICJhbGdvcml0aG0iOiAiSE1BQy1TSEEyNTYiIH0'
        }
      }
    });
    facebook.getAccessTokenFromCode = function(code, redirectUri, callback) {
      assert.equal(code, 'dummy');
      callback(new Error('test'), null);
    };
    facebook.getUserAccessToken(function(err, accessToken) {
      assert.ok(err instanceof Error);
      assert.equal(err.message, 'test');
      assert.equal(accessToken, null);
      assert.equal(facebook.getPersistentData('code'), false);
      assert.equal(facebook.getPersistentData('access_token'), false);

      facebook.getAccessTokenFromCode = function(code, redirectUri, callback) {
        callback(null, 'dummy-access-token');
      };
      facebook.getUserAccessToken(function(err, accessToken) {
        assert.equal(err, null);
        assert.equal(accessToken, 'dummy-access-token');
        assert.equal(facebook.getPersistentData('code'), 'dummy');
        assert.equal(facebook.getPersistentData('access_token'), 'dummy-access-token');

        facebook.getAccessTokenFromCode = function(code, redirectUri, callback) {
          callback(null, false);
        };

        facebook.getUserAccessToken(function(err, accessToken) {
          assert.equal(err, null);
          assert.equal(accessToken, false);
          assert.equal(facebook.getPersistentData('code'), false);
          assert.equal(facebook.getPersistentData('access_token'), false);

          facebook = new TransientFacebook({
            appId: 'dummy',
            secret: 'secret-dummy',
            request: {
              connection: {},
              headers: {},
              body: {
                signed_request: 'Sy3mhK4xP9RWsN905MP1sJrkbkGrXgz2y7r-Fx6lqBU.eyAiYWxnb3JpdGhtIjogIkhNQUMtU0hBMjU2IiB9'
              }
            }
          });

          facebook.setPersistentData('code', 'bad data');
          facebook.setPersistentData('access_token', 'bad data');
          facebook.getUserAccessToken(function(err, accessToken) {
            assert.equal(err, null);
            assert.equal(accessToken, false);
            assert.equal(facebook.getPersistentData('code'), false);
            assert.equal(facebook.getPersistentData('access_token'), false);

            facebook = new TransientFacebook({
              appId: 'dummy',
              secret: 'secret-dummy',
              request: {
                connection: {},
                headers: {},
                query: {
                  code: 'dummy-code',
                  state: 'dummy-state'
                }
              },
              store: {
                state: 'dummy-state'
              }
            });

            var responseReturned = false;
            facebook.getAccessTokenFromCode = function(code, redirectUri, callback) {
              assert.equal(err, null);
              assert.equal(code, 'dummy-code');
              responseReturned = true;
              callback(null, false);
            };
            facebook.getUserAccessToken(function(err, accessToken) {
              assert.equal(err, null);
              assert.equal(accessToken, false);
              assert.ok(responseReturned);
              done = true;
            });
          });
        });
      });
    });
  },

  getUserFromAvailableData: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var facebook = new TransientFacebook({
      appId: 'dummy',
      secret: 'secret-dummy',
      request: {
        connection: {},
        headers: {},
        body: {
          signed_request: 'Sy3mhK4xP9RWsN905MP1sJrkbkGrXgz2y7r-Fx6lqBU.eyAiYWxnb3JpdGhtIjogIkhNQUMtU0hBMjU2IiB9'
        }
      }
    });
    facebook.getUserFromAvailableData(function(err, user) {
      assert.equal(err, null);
      assert.equal(user, 0);

      facebook = new TransientFacebook({
        appId: config.appId,
        secret: config.secret,
        request: {
          connection: {},
          headers: {},
          body: {}
        }
      });

      facebook.getAccessToken = function(callback) {
        callback(new Error('test'), null);
      };
      facebook.getUserFromAvailableData(function(err, user) {
        assert.ok(err instanceof Error);
        assert.equal(user, null);
        assert.equal(err.message, 'test');

        facebook = new TransientFacebook({
          appId: config.appId,
          secret: config.secret,
          request: {
            connection: {},
            headers: {},
            body: {}
          },
          store: {
            access_token: 'dummy'
          }
        });

        var called = false;
        facebook.api = function(method, callback) {
          assert.equal(method, '/me');
          called = true;
          callback(new Error('test'), null);
        };
        facebook.getUserFromAvailableData(function(err, user) {
          assert.ok(called);
          assert.equal(err, null);
          assert.equal(user, 0);

          var appToken = facebook.getApplicationAccessToken();

          facebook = new TransientFacebook({
            appId: config.appId,
            secret: config.secret,
            request: {
              connection: {},
              headers: {},
              body: {}
            },
            store: {
              access_token: appToken
            }
          });

          var called0 = false;
          facebook.api = function(method, callback) {
            called0 = true;
            callback(new Error('test'), null);
          };
          facebook.getUserFromAvailableData(function(err, user) {
            assert.equal(called0, false);
            assert.equal(err, null);
            assert.equal(user, 0);

            facebook = new TransientFacebook({
              appId: config.appId,
              secret: config.secret,
              request: {
                connection: {},
                headers: {},
                body: {}
              },
              store: {
                access_token: 'dummy_access_token'
              }
            });
            facebook.getUserFromAccessToken = function(callback) {
              callback(new Error('error-message'), null);
            };
            facebook.getUserFromAvailableData(function(err, user) {
              assert.notEqual(err, null);
              assert.equal(err.message, 'error-message');
              assert.equal(user, null);

              facebook.getUserFromAccessToken = function(callback) {
                callback(null, '1323');
              };
              facebook.getUserFromAvailableData(function(err, user) {
                assert.equal(err, null);
                assert.equal(user, '1323');
                done = true;
              });
            });
          });
        });
      });
    });
  },

  isVideoPost: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    assert.equal(facebook.isVideoPost('/me/videos'), false);
    assert.equal(facebook.isVideoPost('/foo/videos', 'GET'), false);
    assert.equal(facebook.isVideoPost('/bar/videos', 'POST'), true);
    assert.equal(facebook.isVideoPost('/me/videossss', 'POST'), false);
    assert.equal(facebook.isVideoPost('/videos', 'POST'), false);
    assert.equal(facebook.isVideoPost('/baz', 'POST'), false);

    done = true;
  },

  requestToGraphVideoDomain: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.makeRequest = function(host, path, params, callback) {
      assert.equal(host, 'graph-video.facebook.com');
      callback(null, '{ "test": "ok" }');
    };

    facebook.graph('/amachang/videos', 'POST', function(err, data) {
      assert.equal(err, null);
      assert.equal(data.test, 'ok');
    });

    facebook.graph('/foo/videos', 'POST', function(err, data) {
      assert.equal(err, null);
      assert.equal(data.test, 'ok');
    });

    facebook.makeRequest = function(host, path, params, callback) {
      assert.equal(host, 'graph.facebook.com');
      callback(null, '{ "test": "ok" }');
    };

    facebook.graph('/bar/videossss', 'POST', function(err, data) {
      assert.equal(err, null);
      assert.equal(data.test, 'ok');
    });

    facebook.graph('/videos', 'POST', function(err, data) {
      assert.equal(err, null);
      assert.equal(data.test, 'ok');
    });

    facebook.graph('/baz/videos', 'GET', function(err, data) {
      assert.equal(err, null);
      assert.equal(data.test, 'ok');
    });

    done = true;
  },

  graph: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.graph('/amachang', function(err, data) {
      assert.equal(err, null);
      assert.equal(data.id, '1055572299');
      facebook.graph('/amachang', 'POST', function(err, data) {
        assert.ok(err instanceof Error);
        assert.equal(data, null);
        facebook.graph('/', { ids: 'amachang,yukoba' }, function(err, data) {
          assert.equal(data.amachang.id, '1055572299');
          assert.equal(data.yukoba.id, '1192222589');
          facebook.graph('invalid path', function(err, data) {
            assert.ok(err instanceof Error);
            assert.equal(data, null);
            done = true;
          });
        });
      });
    });
  },

  destroySession: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204',
      request: {
        headers: {
          host: 'www.test.com'
        },
        cookies: {
            fbsr_117743971608120: '1sxR88U4SW9m6QnSxwCEw_CObqsllXhnpP5j2pxD97c.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImV4cGlyZXMiOjEyODEwNTI4MDAsIm9hdXRoX3Rva2VuIjoiMTE3NzQzOTcxNjA4MTIwfDIuVlNUUWpub3hYVVNYd1RzcDB1U2g5d19fLjg2NDAwLjEyODEwNTI4MDAtMTY3Nzg0NjM4NXx4NURORHBtcy1nMUM0dUJHQVYzSVdRX2pYV0kuIiwidXNlcl9pZCI6IjE2Nzc4NDYzODUifQ'
        }
      }
    });

    assert.notEqual(facebook.getSignedRequest(), null);
    facebook.destroySession();
    assert.equal(facebook.getSignedRequest(), null);

    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204',
      request: {
        headers: {
          host: 'www.test.com'
        },
        cookies: {
            fbsr_117743971608120: '1sxR88U4SW9m6QnSxwCEw_CObqsllXhnpP5j2pxD97c.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImV4cGlyZXMiOjEyODEwNTI4MDAsIm9hdXRoX3Rva2VuIjoiMTE3NzQzOTcxNjA4MTIwfDIuVlNUUWpub3hYVVNYd1RzcDB1U2g5d19fLjg2NDAwLjEyODEwNTI4MDAtMTY3Nzg0NjM4NXx4NURORHBtcy1nMUM0dUJHQVYzSVdRX2pYV0kuIiwidXNlcl9pZCI6IjE2Nzc4NDYzODUifQ'
        }
      },
      response: {
        clearCookie: function(cookieName, options) {
          clearCookieLogs.push({
            name: cookieName,
            path: options.path,
            domain: options.domain
          });
        }
      }
    });

    var clearCookieLogs = [];

    facebook.destroySession();

    assert.equal(clearCookieLogs.length, 1);
    assert.equal(clearCookieLogs[0].name, 'fbsr_117743971608120');
    assert.equal(clearCookieLogs[0].path, '/');
    assert.equal(clearCookieLogs[0].domain, '.www.test.com');

    var facebook = new TransientFacebook({
      appId: '117743971608120',
      secret: '943716006e74d9b9283d4d5d8ab93204',
      request: {
        headers: {
          host: 'www.test.com'
        },
        cookies: {
          fbsr_117743971608120: '1sxR88U4SW9m6QnSxwCEw_CObqsllXhnpP5j2pxD97c.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImV4cGlyZXMiOjEyODEwNTI4MDAsIm9hdXRoX3Rva2VuIjoiMTE3NzQzOTcxNjA4MTIwfDIuVlNUUWpub3hYVVNYd1RzcDB1U2g5d19fLjg2NDAwLjEyODEwNTI4MDAtMTY3Nzg0NjM4NXx4NURORHBtcy1nMUM0dUJHQVYzSVdRX2pYV0kuIiwidXNlcl9pZCI6IjE2Nzc4NDYzODUifQ',
          fbm_117743971608120: 'base_domain=basedomain.test.com'
        }
      },
      response: {
        clearCookie: function(cookieName, options) {
          clearCookieLogs.push({
            name: cookieName,
            path: options.path,
            domain: options.domain
          });
        }
      }
    });

    clearCookieLogs = [];

    facebook.destroySession();

    assert.equal(clearCookieLogs.length, 1);
    assert.equal(clearCookieLogs[0].name, 'fbsr_117743971608120');
    assert.equal(clearCookieLogs[0].path, '/');
    assert.equal(clearCookieLogs[0].domain, 'basedomain.test.com');

    done = true;
  },

  makeRequest: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });
    var facebook = new TransientFacebook({
      appId: config.appId,
      secret: config.secret
    });

    facebook.makeRequest('graph.facebook.com', '/amachang', { method: 'GET' }, function(err, data) {
      assert.equal(err, null);
      assert.notEqual(data, null);
      assert.equal(JSON.parse(data).id, '1055572299');
      done = true;
    });
  }
};

function TransientFacebook(params) {
  this.store = this.mergeObject({}, params.store || {});
  BaseFacebook.apply(this, arguments);
};

util.inherits(TransientFacebook, BaseFacebook);

TransientFacebook.prototype.setPersistentData = function(key, value) {
  this.store[key] = value;
};

TransientFacebook.prototype.getPersistentData = function(key, defaultValue) {
  return this.store.hasOwnProperty(key) ? (this.store[key]) : (defaultValue === undefined ? false : defaultValue);
};

TransientFacebook.prototype.clearPersistentData = function(key) {
  delete this.store[key];
};

TransientFacebook.prototype.clearAllPersistentData = function() {
  this.store = {};
};

function isArray(ar) {
  return Array.isArray(ar) || (typeof ar === 'object' && Object.prototype.toString.call(ar) === '[object Array]');
}

