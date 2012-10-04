
// Flowlet detail view

define([
	'lib/text!../partials/flowletdetail.html'
	], function (Template) {
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		__timeout: null,
		show: function (current) {

			var self = this;
			var controller;

			this.set('controller', controller = Em.Object.create({
				current: current,
				inputs: [],
				outputs: [],
				getStats: function () {

					C.get.apply(this.get('current'), this.get('current').getUpdateRequest());

					self.__timeout = setTimeout(function () {
						if (self.get('controller')) {
							self.get('controller').getStats();
						}
					}, 1000);

				}
			}));

			this.get('controller').getStats();
			$('#flowlet-container').show();

			var cx = C.Ctl.Flow.current.connections;
			function find_contributors(direction, flowlet, input) {
				var res = [];
				var opp = 'from';
				if (direction === 'from') {
					opp = 'to';
				}
				for (var i = 0; i < cx.length; i ++) {
					if (cx[i][direction]['flowlet'] === flowlet &&
						cx[i][direction]['stream'] === input) {
						res.push({name: cx[i][opp]['flowlet']});
					}
				}
				return res;
			}

			var streams = C.Ctl.Flow.current.flowletStreams[current.name];
			for (var i in streams) {
				if (streams[i].second === 'IN') {
					controller.inputs.push({
						'name': i,
						'contrib': find_contributors('to', current.name, i)
					});
				}
			}
			for (var i in streams) {
				if (streams[i].second === 'OUT') {
					controller.outputs.push({
						'name': i,
						'contrib': find_contributors('from', current.name, i)
					});
				}
			}

			this.select('processed');

		},
		select: function (event) {

			var tabName;

			if (typeof event === 'string') {
				tabName = event;
			} else {
				tabName = $(event.target).attr('tab-name');
			}

			$('#flowlet-popup-inputs').hide();
			$('#flowlet-popup-inputs-tab').removeClass('tab-selected');
			$('#flowlet-popup-processed').hide();
			$('#flowlet-popup-processed-tab').removeClass('tab-selected');
			$('#flowlet-popup-outputs').hide();
			$('#flowlet-popup-outputs-tab').removeClass('tab-selected');

			$('#flowlet-popup-' + tabName).show();
			$('#flowlet-popup-' + tabName + '-tab').addClass('tab-selected');

		},
		close: function () {
			$('#flowlet-container').hide();
			this.set('current', null);

			clearTimeout(this.__timeout);

		}
	});
});