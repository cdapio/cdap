
define([
	'lib/text!../../templates/flow.html',
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template),
		current: null,
		name: function () {
			if (this.get('current')) {
				return this.get('current').get('name');
			} else {
				return '&nbsp;';
			}
		}.property('current')
	});

});