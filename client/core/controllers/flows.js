
define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		load: function () {

			var self = this;
			this.clear();

			App.interstitial.loading();

			App.socket.request('manager', {
				method: 'getFlows',
				params: ['demo']
			}, function (error, response) {

				if (error) {
					App.interstitial.label('Unable to connect to FlowMonitor.');
					return;
				}

				var flows = response.params;
				if (!flows) {
					return;
				}

				for (var i = 0; i < flows.length; i ++) {
					flows[i] = App.Models.Flow.create(flows[i]);
					App.Controllers.Flows.pushObject(flows[i]);
				}

				App.interstitial.hide();
				self.interval = setInterval(function () {
					self.refresh();
				}, 1000);

			});


		},
		unload: function () {
			clearInterval(this.interval);
		},
		refresh: function () {

			if (this.pending) {
				return;
			}

			App.socket.request('manager', {
				method: 'getFlows',
				params: ['demo']
			}, function (error, response) {

				var flows = response.params;
				if (!flows) {
					return;
				}

				var content = App.Controllers.Flows.content;

				for (var i = 0; i < flows.length; i ++) {

					for (var j = 0; j < content.length; j ++) {

						if (content[j].applicationId === flows[i].applicationId &&
							content[j].flowId === flows[i].flowId) {

							// Avoid flickering state when starting.
							if (content[j].currentState === 'STARTING' &&
								flows[i].currentState === 'STOPPED' ||
								content[j].currentState === 'STARTING' &&
								flows[i].currentState === 'UNDEPLOYED') {
								continue;
							}

							// Avoid flickering state when stopping.
							if (content[j].currentState === 'STOPPING' &&
								flows[i].currentState === 'RUNNING') {
								continue;
							}

							content[j].set('lastStarted', flows[i].lastStarted);
							content[j].set('lastStopped', flows[i].lastStopped);
							content[j].set('currentState', flows[i].currentState);
						}
					}
				}
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

			App.Controllers.Flows.pending = true;
			thisFlow.set('currentState', 'STARTING');

			App.socket.request('manager', {
				method: 'start',
				params: [app, id, version]
			}, function (error, response) {

				App.Controllers.Flows.pending = false;

				if (App.Controllers.Flow.current) {
					App.Controllers.Flow.set('currentRun', response.params.id);
				}

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

			App.Controllers.Flows.pending = true;
			thisFlow.set('currentState', 'STOPPING');

			App.socket.request('manager', {
				method: 'stop',
				params: [app, id, version]
			}, function (error, response) {

				App.Controllers.Flows.pending = false;

				if (App.Controllers.Flow.current) {

					App.Controllers.Flow.history.unshiftObject(App.Models.Run.create({
						"runId": response.params.id,
						"endStatus": "STOPPED",
						"startTime": thisFlow.get('lastStarted'),
						"endTime": new Date().getTime() / 1000
					}));
					App.Controllers.Flow.history.popObject();

					App.Controllers.Flow.stop_spin();
				}

				thisFlow.set('runs', thisFlow.runs + 1);
				thisFlow.set('lastStopped', new Date().getTime() / 1000);

			});

		}
	});
});