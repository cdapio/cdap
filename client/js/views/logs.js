
define([
	'lib/text!../../templates/logs.html'
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template)
	});

});