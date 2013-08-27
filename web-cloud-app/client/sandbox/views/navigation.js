
define(['core/lib/text!sandbox/partials/navigation.html'], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template)
	}).appendTo($('#navi'));

});