FACEBOOK_COOKIE_PREFIX = 'fbsr_';

var crypto = require('crypto');

module.exports = function(app, secret) {
	return function(req, res, next) {
		if(req.header("X-" + FACEBOOK_COOKIE_PREFIX + app)) {
			req.cookies[FACEBOOK_COOKIE_PREFIX + app] = req.header("X-" + FACEBOOK_COOKIE_PREFIX + app);
		}

		var cookie = req.cookies[FACEBOOK_COOKIE_PREFIX + app];

		req.facebook = {};

		if(cookie) {
			var chunks = cookie.split('.', 2);

			var rawSignature = chunks[0].replace(/\-/g, '+').replace(/\_/g, '/');
			var hexSignature = encodeToHex(new Buffer(rawSignature, 'base64'));

	        var rawToken = new Buffer(chunks[1].replace(/\-/g, '+').replace(/\_/g, '/'), 'base64').toString();
	        var token = JSON.parse(rawToken);

	        var hmac = crypto.createHmac('sha256', secret);
        	hmac.update(chunks[1]);

        	var expectedSignature = hmac.digest('hex');

        	if(expectedSignature == hexSignature) {
        		req.facebook = {session: token};
        	}
		}
		
		next();
	}
}


function encodeToHex(buffer){
	var toHex = function (n) {
	    return ((n < 16) ? '0' : '') + n.toString(16);
	}

	var hex_string = '';
	var length = buffer.length;
	for (var i = 0; i < length; i++) {
	    hex_string += toHex(buffer[i]);
	}
	return hex_string;
}