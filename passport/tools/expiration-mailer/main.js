#!/usr/bin/env node

var fs  = require("fs"),
	log4js = require('log4js'),
	path = require('path'),
	csv = require('csv'),
	nodemailer = require('nodemailer'),
	program = require('commander')

program.version('0.0.1')
	.option('-f, --force', 'force all emails without user intervention');
program.parse(process.argv);

const FORCE = program.force;

/**
 * Configure the logger.
 */
var LOG_LEVEL = 'ALL';
log4js.configure({
	appenders: [
		{
			type : 'console'
		}
	]
});
var logger = process.logger = log4js.getLogger('Emailer');
logger.setLevel(LOG_LEVEL);

/**
 * Configure the emailer.
 */

var emailTransport = nodemailer.createTransport("SMTP", {
	host: "smtp.sendgrid.net",
	secureConnection: true,
	port: 465,
	auth: {
		user: 'dmosites',
		pass: 'aF$2sSJ#2p!lskU'
	}
});

var emailDir = path.join(__dirname, 'email-templates');

const FROM_EMAIL = 'Continuuity <info@continuuity.com>';
var subjects = {
	90: 'Your Continuuity Developer Sandbox Expires Today',
	83: 'Continuuity Developer Sandbox Expiration Notice',
	76: 'Continuuity Developer Sandbox Expiration Notice',
	60: 'Using Your Continuuity Developer Sandbox',
	'special': 'Using Your Continuuity Developer Sandbox'
};

/**
 * Send an email!
 */
function sendEmail (kind, email_id, locals, done) {

	// Stringify
	kind += '';

	logger.trace('Sending ' + kind + ' warning to ' + locals.first_name + ' ' + locals.last_name + ' (' + email_id + ')');

	var text = fs.readFileSync(__dirname + '/email-templates/' + kind + '/text.ejs', 'utf8');
	var html = fs.readFileSync(__dirname + '/email-templates/' + kind + '/html.ejs', 'utf8');

	emailTransport.sendMail({
		from: FROM_EMAIL,
		to: email_id,
		subject: subjects[kind],
		text: text,
		html: html
	}, function(err, responseStatus) {
		if (err) {
			logger.warn(err);
			done(500, err);
		} else {
			done(200, true);

		}
	});

}

/**
 * Send a bunch of emails!
 */
function sendEmails(recipients, template) {

	var completed = 0, errors = 0;
	for (var i = 0; i < recipients.length; i ++) {

		var kind = recipients[i][0];
		var email_id = recipients[i][1].email;
		var locals = recipients[i][1];

		sendEmail(kind, email_id, locals, function (status, error) {

			if (status === 500) {
				errors++;
			}

			completed ++;

			if (completed === recipients.length) {

				logger.info('Completed successfully! ' + completed + ' recipients received an email. ' + (errors ? errors + ' did not.' : ''));
				process.exit();

			}

		});
	}


}

var days = [76, 83, 90, 'special'];
var recipients = [];
var remaining = days.length - 1;

/**
 * Load up the recipients for the day!
 */
var today = new Date();
logger.trace('Searching for today\'s recipients... ' + today.toString());

var pathName = '/opt/continuuity/data';

for (var i = 0; i < days.length; i ++) {

	var fileName = 'vpcExpirationReport-' + (today.getYear() + 1900) + (today.getMonth() < 9 ? '0' : '') +
		(today.getMonth() + 1) + today.getDate() + '_' + days[i];

	if (fs.existsSync(pathName + '/' + fileName)) {

		(function () {

			var entries = [];
			var day = days[i];

			csv().from.stream(fs.createReadStream(pathName + '/' + fileName))
			.on('record', function(row,index){
				entries.push(row);
			}).on('end', function (count) {

				var Table = require('cli-table');
				var table = new Table({ head: ["First", "Last", "Email", "Sandbox Name"] });

				for (var j = 0; j < count; j ++) {

					table.push(entries[j]);

					recipients.push([day, {
						'first_name': entries[j][0],
						'last_name': entries[j][1],
						'email': entries[j][2],
						'sandbox': entries[j][3]
					}]);

				}

				logger.info('The following will receive a ' + day + '-day notice:');
				console.log(table.toString() + '\n');

				if (!remaining--) {

					if (!recipients.length) {
                                                logger.warn('No emails need to be sent.');
                                                process.exit();
                                        }

					setTimeout(function () {

						if (FORCE) {
							sendEmails(recipients, day);
						} else {
							program.confirm('continue? ', function(ok){
								if (ok) {
									console.log('\n');
									sendEmails(recipients, day);
								} else {
									logger.info('Cancelled');
									process.exit();
								}
							});
						}

					}, 1000);

				}

			});

		})();

	} else {

		logger.error('File does not exist!', fileName);
		remaining --;

	}

}
