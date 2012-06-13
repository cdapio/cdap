
define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		load: function () {

			this.clear();

			App.interstitial.loading();

			App.socket.request('monitor', {
				method: 'getFlows',
				params: ['demo']
			}, function (response) {

				var flows = response.params;
				if (!flows) {
					return;
				}

				for (var i = 0; i < flows.length; i ++) {
					flows[i] = App.Models.Flow.create(flows[i]);
					App.Controllers.Flows.pushObject(flows[i]);
				}

				App.interstitial.hide();
			});
		},
		start: function (app, id, version) {

			$('#flow-alert').hide();

			var thisFlow;
			if (App.Controllers.Flow.current) {
				thisFlow = App.Controllers.Flow.current;
			} else {
				var flows = App.Controllers.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].get('applicationId') === app &&
						flows[i].get('flowId') === id) {
							thisFlow = flows[i];
							break;
					}
				}
			}

			thisFlow.set('currentState', 'STARTING');

			App.socket.request('manager', {
				method: 'start',
				params: [app, id, version]
			}, function (response) {

				if (App.Controllers.Flow.current) {
					App.Controllers.Flow.set('currentRun', response.params.id);
				}

				thisFlow.set('currentState', 'RUNNING');
				thisFlow.set('lastStarted', new Date().getTime() / 1000);

				if (App.Controllers.Flow.current) {
					App.Controllers.Flow.updateStats();
				}

			});

		},
		stop: function (app, id, version) {

			$('#flow-alert').hide();

			var thisFlow;
			if (App.Controllers.Flow.current) {
				thisFlow = App.Controllers.Flow.current;
			} else {
				var flows = App.Controllers.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].get('applicationId') === app &&
						flows[i].get('flowId') === id) {
							thisFlow = flows[i];
							break;
					}
				}
			}

			thisFlow.set('currentState', 'STOPPING');

			App.socket.request('manager', {
				method: 'stop',
				params: [app, id, version]
			}, function (response) {

				if (App.Controllers.Flow.current) {

					App.Controllers.Flow.history.pushObject(App.Models.Run.create({
						"runId": response.params.id,
						"endStatus": "STOPPED",
						"startTime": thisFlow.get('lastStarted'),
						"endTime": new Date().getTime() / 1000
					}));

					App.Controllers.Flow.stop_spin();
				}

				thisFlow.set('runs', thisFlow.runs + 1);
				thisFlow.set('currentState', 'STOPPED');
				thisFlow.set('lastStopped', new Date().getTime() / 1000);

			});

		}
	});
});