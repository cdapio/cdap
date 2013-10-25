var util = require('util');
var crypto = require('crypto');
var assert = require('assert');

assert.equal(process.argv.length, 3, 'Command line arguments length must be 3');
assert.ok('TEST_FB_SECRET' in process.env);

console.log(createSignedRequest(process.argv[2], process.env.TEST_FB_SECRET));

function createSignedRequest(json, secret) {
  var payload = encodeBase64Url(json);

  var hmac = crypto.createHmac('sha256', secret);
  hmac.update(payload);
  var encodedSig = hmac.digest('base64');
  encodedSig = base64ToBase64Url(encodedSig);

  return encodedSig + '.' + payload;
}

function encodeBase64Url(str) {
  var buffer = new Buffer(str, 'utf8');
  var base64 = buffer.toString('base64');
  var base64url = base64ToBase64Url(base64);
  return base64url;
}

function base64ToBase64Url(base64) {
  var base64url = base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
  return base64url;
}

