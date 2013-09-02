
define(['core/lib/text!local/partials/navigation.html'], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template)
	}).appendTo($('#navi'));

});