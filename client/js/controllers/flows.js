
define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		load: function () {

			this.clear();

			App.socket.request('rest', {
				method: 'flows'
			}, function (response) {
				var flows = response.params;
				if (!flows) {
					return;
				}

				for (var i = 0; i < flows.length; i ++) {
					flows[i] = App.Models.Flow.create(flows[i]);
					App.Controllers.Flows.pushObject(flows[i]);
				}
			});
		},
		start: function (id) {
			App.socket.request('rest', {
				method: 'start',
				params: id
			}, function (response) {

				if (App.Controllers.Flow.current) {
					App.Controllers.Flow.current.set('status', 'running');
					App.Controllers.Flow.current.set('running', new Date().ISO8601());
				}

				var flows = App.Controllers.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].id === id) {
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
				method: 'stop',
				params: id
			}, function (response) {

				if (App.Controllers.Flow.current) {
					App.Controllers.Flow.current.set('status', 'stopped');
					App.Controllers.Flow.current.set('stopped', new Date().ISO8601());

					App.Controllers.Flow.history.pushObject(Em.Object.create({
						"name": 'Flow Run',
						"result": "success",
						"user": "dmosites",
						"time": new Date().ISO8601()
					}));

					App.Controllers.Flow.stop_spin();
				}

				var flows = App.Controllers.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].id === id) {
						flows[i].set('status', 'stopped');
						flows[i].set('stopped', new Date().ISO8601());
						return;
					}
				}
			});
		},
		detail: function (id) {
			App.router.set('location', '/flows/' + id);
		}

	});

});