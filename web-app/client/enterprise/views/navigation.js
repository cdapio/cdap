
define(['core/lib/text!enterprise/partials/navigation.html'], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template)
	}).appendTo($('#navi'));

});