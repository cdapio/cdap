
define(['lib/text!../partials/navigation.html'], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template)
	}).appendTo($('#navi'));

});