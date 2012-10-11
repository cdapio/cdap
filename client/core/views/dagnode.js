
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
					return (current.get('type') === 'Stream' ? ' source' : '');
				}
				else {
					return 'unknown';
				}
			}.property(),
			click: function (event) {
				var viz;

				if (this.get('current').get('type') === 'Stream') {
					viz = C.router.applicationController.view.get('stream-detail');
				} else {
					viz = C.router.applicationController.view.get('flowlet-detail');
				}
				
				viz.show(this.get('current'));

			}
		});
	});