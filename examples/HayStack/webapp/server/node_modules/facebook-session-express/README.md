## Facebook Cookie (FBSR) parser for Express/Connect
Quick and dirty implemenentation (maintained and poor yet) for parsing and setting 
the Facebook Session Cookie (FBSR) on the on going request.

#### Installation
	
	[sudo] npm install facebook-session-express

#### Sample Usage

	var facebook = require('facebook-session-express');

	var FACEBOOK_APP_ID = '{your_app_id}',
    	FACEBOOK_SECRET = '{your_app_secret}';

    
    app.get("/", facebook(FACEBOOK_APP_ID, FACEBOOK_SECRET), function(req, res){
    	var user = req.facebook.session; /* this is how you access your session */
    });

### Requirements

Proudly built using node.js using npm.


### Meta

Written by [@johnnyhalife](http://twitter.com/johnnyhalife) for Tactivos under `do the whatever you want license`