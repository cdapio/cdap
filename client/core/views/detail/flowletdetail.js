
// Flowlet detail view

define([
	'lib/text!../../partials/flowletdetail.html'
	], function (Template) {
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		classNames: ['popup-modal'],
		__timeout: null,
		controller: Em.Object.create({
			current: null,
			inputs: [],
			outputs: [],
			getStats: function (self) {

				C.get.apply(this.get('current'), this.get('current').getUpdateRequest());

				self.__timeout = setTimeout(function () {
					if (self.get('controller')) {
						self.get('controller').getStats(self);
					}
				}, 1000);

			}
		}),
		show: function (current) {

			var self = this;
			var controller = this.get('controller');

			controller.set('current', current);
			controller.getStats(self);

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
						res.push({name: cx[i][opp]['flowlet'] || cx[i][opp]['stream']});
					}
				}
				return res;
			}

			var streams = C.Ctl.Flow.current.flowletStreams[current.name],
				inputs = [], outputs = [];

			for (var i in streams) {
				if (streams[i].second === 'IN') {
					inputs.push({
						'name': i,
						'contrib': find_contributors('to', current.name, i)
					});
				}
			}
			controller.set('inputs', inputs);

			for (var i in streams) {
				if (streams[i].second === 'OUT') {
					outputs.push({
						'name': i,
						'contrib': find_contributors('from', current.name, i)
					});
				}
			}
			controller.set('outputs', outputs);

			this.select('inputs');

			$(this.get('element')).fadeIn();

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

			this.set('current', null);
			$(this.get('element')).hide();
			clearTimeout(this.__timeout);

		},
		navigate: function (event) {

			var id = $(event.target).attr('flowlet-id');
			var flowlet = C.Ctl.Flow.get_flowlet(id);
			
			this.close();
			this.show(flowlet);
			
		},
		addOneInstance: function () {
			this.confirm('Add 1 instance to ', +1);
		},
		removeOneInstance: function () {
			this.confirm('Remove 1 instance from ', -1);
		},
		confirm: function (message, value) {

			var current = this.get('controller.current');
			var name = current.name;

			C.Vw.Modal.show(
				"Flowlet Instances",
				message + '"' + name + '" flowlet?',
				function () {
					current.addInstances(value, function () {
					
					});
				});

		}
	});
});