
define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		load: function () {

			this.clear();

			App.socket.request('rest', {
				method: 'flows'
			}, function (response) {
				var flows = response.params;
				for (var i = 0; i < flows.length; i ++) {
					flows[i] = App.Models.Flow.create(flows[i]);
					App.Controllers.Flows.pushObject(flows[i]);
				}
				$('.timeago').timeago();
			});
		},
		start: function (id) {
			App.socket.request('rest', {
				method: 'flows',
				params: id + '/start'
			}, function (response) {
				var flows = App.Controllers.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].id == id) {
						flows[i].set('status', 'running');
						flows[i].set('started', new Date().ISO8601());
						flows[i].set('runs', flows[i].runs + 1);
						return;
					}
				}
			});
		},
		stop: function (id) {
			App.socket.request('rest', {
				method: 'flows',
				params: id + '/stop'
			}, function (response) {
				var flows = App.Controllers.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].id == id) {
						flows[i].set('status', 'stopped');
						flows[i].set('stopped', new Date().ISO8601());
						return;
					}
				}
			});
		},
		detail: function (id) {
			App.router.set('location', 'flows/' + id);
		}

	});

});