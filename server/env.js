//
// Server environment configuration
//

var getNetworkIPs = (function () {
    var ignoreRE = /^(127\.0\.0\.1|::1|fe80(:1)?::1(%.*)?)$/i;

    var exec = require('child_process').exec;
    var cached;
    var command;
    var filterRE;

    switch (process.platform) {
    case 'win32':
    //case 'win64': // TODO: test
        command = 'ipconfig';
        filterRE = /\bIP(v[46])?-?[^:\r\n]+:\s*([^\s]+)/g;
        // TODO: find IPv6 RegEx
        break;
    case 'darwin':
        command = 'ifconfig';
        filterRE = /\binet\s+([^\s]+)/g;
        // filterRE = /\binet6\s+([^\s]+)/g; // IPv6
        break;
    default:
        command = 'ifconfig';
        filterRE = /\binet\b[^:]+:\s*([^\s]+)/g;
        // filterRE = /\binet6[^:]+:\s*([^\s]+)/g; // IPv6
        break;
    }

    return function (callback, bypassCache) {
        if (cached && !bypassCache) {
            callback(null, cached);
            return;
        }
        // system call
        exec(command, function (error, stdout, sterr) {
            cached = [];
            var ip;
            var matches = stdout.match(filterRE) || [];
            //if (!error) {
            for (var i = 0; i < matches.length; i++) {
                ip = matches[i].replace(filterRE, '$1');
                if (!ignoreRE.test(ip)) {
                    cached.push(ip);
                }
            }
            //}
            callback(error, cached);
        });
    };
})();

(function () {

	this.configure = function (app, express, io, done) {

		console.log('Configuring for ' + process.env.NODE_ENV);

		if (process.env.NODE_ENV === 'development') {
			this.PORT = 9999;
		} else {
			this.PORT = 80;

			this.USERNAMES = ['payvment', 'continuuity'];
			this.PASSWORDS = ['pvrealtime123!', 'realwh00p'];
		}

		app.use(express['static'](__dirname + '/../client/'));

		console.log('Using static directory ', __dirname + '/../client/');

		var api = this.api = require('./api');
		var fs = require('fs'),
			xml2js = require('xml2js');
		var parser = new xml2js.Parser();

		function getVersion (done) {
			fs.readFile('/opt/continuuity/VERSION', 'utf8', function (error, result) {
				if (error) {
					console.log('Could not find VERSION file. /opt/continuuity/VERSION');
					done('UNKNOWN');
				} else {
					console.log('VERSION: ', result);
					done(result);
				}
			});
		}

		fs.readFile((process.env.CONTINUUITY_HOME || './server') + '/conf/continuuity-site.xml',
			function (err, result) {

				if (err) {
					console.log('COULD NOT OPEN CONFIG. Defaulting to localhost for all services. (' + (process.env.CONTINUUITY_HOME || './server') +
						'/conf/continuuity-site.xml)');

					var config = {};

					// Upload
					config['resource.manager.server.port'] = 45000;
					config['resource.manager.server.address'] = '127.0.0.1';
					// MetaData
					config['metadata.server.port'] = 45004;
					config['metadata.server.address'] = '127.0.0.1';
					// Manager
					config['flow.manager.server.port'] = 45001;
					config['flow.manager.server.address'] = '127.0.0.1';
					// Monitor
					config['flow.monitor.server.port'] = 45002;
					config['flow.monitor.server.address'] = '127.0.0.1';
					// Gateway
					config['gateway.port'] = 10000;
					config['gateway.hostname'] = '127.0.0.1';

					getVersion(function (version) {

						getNetworkIPs(function (error, ip) {
						
							api.configure(config, version, ip);
							done(true);

						}, false);

					});

				} else {
					parser.parseString(result, function (err, result) {

						var config = {};
						result = result.property;

						for (var item in result) {
							item = result[item];
							config[item.name] = item.value;
						}

						// Upload
						config['resource.manager.server.port'] = 45000;
						// MetaData
						config['metadata.server.port'] = 45004;
						// Manager
						config['flow.manager.server.port'] = 45001;
						// Monitor
						config['flow.monitor.server.port'] = 45002;
						// Gateway
						config['gateway.port'] = 10000;

						getVersion(function (version) {

							getNetworkIPs(function (error, ip) {
							
								api.configure(config, version, ip);
								done(true);
								
							}, false);

						});

					});
				}

			});

		/*
		if (process.env.NODE_ENV === 'production') {

			this.USERNAME = 'cntnty';
			this.PASSWORD = 'realtime';

			this.PORT = 80;
			this.api.configure({
				'upload': { 'host': 'upload.continuuity.com', 'port': 45000 },
				'manager': { 'host': 'manager.continuuity.com', 'port': 45001 },
				'monitor': { 'host': 'monitor.continuuity.com', 'port': 45002 },
				'gateway': { 'host': 'stream.continuuity.com', 'port': 10000, 'baseUri': '/rest-stream/' }
			});

		} else if (process.env.NODE_ENV === 'staging') {

			this.USERNAME = 'cntnty';
			this.PASSWORD = 'realtime';

			this.PORT = 80;
			this.api.configure({
				'upload': { 'host': '127.0.0.1', 'port': 45000 },
				'manager': { 'host': '127.0.0.1', 'port': 45001 },
				'monitor': { 'host': '127.0.0.1', 'port': 45002 },
				'gateway': { 'host': '127.0.0.1', 'port': 10000, 'baseUri': '/rest-stream/' }
			});

		} else {

			this.PORT = 9999;
			this.api.configure({
				'upload': { 'host': '127.0.0.1', 'port': 45000 },
				'manager': { 'host': '127.0.0.1', 'port': 45001 },
				'monitor': { 'host': '127.0.0.1', 'port': 45002 },
				'gateway': { 'host': '127.0.0.1', 'port': 10000, 'baseUri': '/rest-stream/' }
			});

		}
		*/

		io.configure('production', function(){
			io.set('transports', [
				'websocket',
				'flashsocket',
				'htmlfile',
				'xhr-polling',
				'jsonp-polling'
				]);
		});
		/*
			The following were commented out for causing problems.
			The file was not served to the client with these on. investigate.
			
			io.enable('browser client minification');
			io.enable('browser client etag');
			io.enable('browser client gzip');
			io.set('log level', 1);
			io.set('transports', [
				'websocket',
				'flashsocket',
				'htmlfile',
				'xhr-polling',
				'jsonp-polling'
				]);
		});
		*/

		io.configure('development', function(){
			io.set('transports', ['websocket']);
		});
	};

}).call(exports);