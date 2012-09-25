define([
	'lib/text!../partials/dagnode.html'
	], function (Template) {

		return Em.View.extend({

			update: function () {

				var type = 'processed.count';

				var sparkline = this.get('sparkline');
				var data = this.get('model').ts[type];

				if (!sparkline) {

					var w = 240,
						h = parseInt(this.get('height'), 10) || 50,
						margin = 8;

					var widget = d3.select(this.get('element'));
					var sparkline = C.util.sparkline(widget, data || [], w, h, margin);

					this.set('sparkline', sparkline);

				} else {

					this.get('sparkline').update(data || []);

				}

			}.observes('model.ts'),

			didInsertElement: function () {
				
				var flowId = this.get('flowId');
				var type = this.get('type');

				var ctl = this.get('controller');
				if (ctl.get('current')) {

					this.set('model', ctl.get('current'));

				} else if (ctl.get('content')) {

					var content = ctl.get('content');

					for (var i = 0; content.length; i ++) {
						if (content[i].get('flowId') === flowId) {

							if (!(type in content[i].ts)) {
								content[i].ts[type] = [];
							}
							this.set('model', content[i]);
							break;

						}
					}
				}

			}.observes('controller.current')
		});
	});