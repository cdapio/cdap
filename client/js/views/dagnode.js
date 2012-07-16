
define([
	'lib/text!../../templates/dagnode.html'
	], function (Template) {

		return Em.View.extend({
			template: Em.Handlebars.compile(Template),
			classNames: ['window'],
			elementId: function () {
				return 'flowlet' + this.get('current').name;
			}.property(),
			className: function () {
				var id = this.get('current').name;
				return ('input-stream' === id ? ' source' : '');
			}.property(),
			click: function (event) {

				var el = $(event.target.parentNode);
				var x = el.offset().left - 56;
				var y = el.offset().top - 48;

				App.Controllers.Flow.set('flowlet', this.get('current'));
				App.Views.Flowlet.show(x, y);
			}
		});
	});