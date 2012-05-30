
define([
	'lib/text!../../templates/dashboard.html'
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template)
	});

});