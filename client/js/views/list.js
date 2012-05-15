
define([
	'lib/text!../../templates/todos.html',
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template)
	});

});