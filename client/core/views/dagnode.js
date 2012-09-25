
define([
	'lib/text!../partials/dagnode.html'
	], function (Template) {

		return Em.View.extend({
			template: Em.Handlebars.compile(Template),
			classNames: ['window'],
			elementId: function () {
				var current;
				if ((current = this.get('current'))) {
					return 'flowlet' + current.name;
				}
				else {
					return 'unknown';
				}
			}.property(),
			classNameBindings: ['className'],
			className: function () {
				var current;
				if ((current = this.get('current'))) {
					return (current.isSource ? ' source' : '');
				}
				else {
					return 'unknown';
				}
			}.property(),
			click: function (event) {

				var el = $(event.target.parentNode);
				var x = el.offset().left - 56;
				var y = el.offset().top - 48;

				this.get("controller").set('flowlet', this.get('current'));
				C.Vw.Flowlet.show(x, y);
			}
		});
	});