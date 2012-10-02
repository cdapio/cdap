
define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		load: function () {

			var self = this;
			this.clear();

			C.interstitial.loading();

			C.socket.request('manager', {
				method: 'getFlows',
				params: []
			}, function (error, response) {

				if (error) {
					C.interstitial.label('Unable to connect to FlowMonitor.');
					return;
				}

				var flows = response.params;
				if (!flows) {
					return;
				}

				for (var i = 0; i < flows.length; i ++) {
					flows[i] = C.Mdl.Flow.create(flows[i]);
					C.Ctl.Flows.pushObject(flows[i]);
				}

				C.interstitial.hide();
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

			C.socket.request('manager', {
				method: 'getFlows',
				params: []
			}, function (error, response) {

				var flows = response.params;
				if (!flows) {
					return;
				}

				var content = C.Ctl.Flows.content;

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
			if (C.Ctl.Flow.current) {
				thisFlow = C.Ctl.Flow.current;
			} else {
				var flows = C.Ctl.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].get('applicationId') === app &&
						flows[i].get('flowId') === id) {
							thisFlow = flows[i];
							break;
					}
				}
			}

			C.Ctl.Flows.pending = true;
			thisFlow.set('currentState', 'STARTING');

			C.socket.request('manager', {
				method: 'start',
				params: [app, id, version]
			}, function (error, response) {

				C.Ctl.Flows.pending = false;

				if (C.Ctl.Flow.current) {
					C.Ctl.Flow.set('currentRun', response.params.id);
				}

				thisFlow.set('lastStarted', new Date().getTime() / 1000);

				if (C.Ctl.Flow.current) {
					C.Ctl.Flow.updateStats();
				}

			});

		},
		stop: function (app, id, version) {

			$('#flow-alert').hide();

			var thisFlow;
			if (C.Ctl.Flow.current) {
				thisFlow = C.Ctl.Flow.current;
			} else {
				var flows = C.Ctl.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].get('applicationId') === app &&
						flows[i].get('flowId') === id) {
							thisFlow = flows[i];
							break;
					}
				}
			}

			C.Ctl.Flows.pending = true;
			thisFlow.set('currentState', 'STOPPING');

			C.socket.request('manager', {
				method: 'stop',
				params: [app, id, version]
			}, function (error, response) {

				C.Ctl.Flows.pending = false;

				if (!response.params) {
					return;
				}

				if (C.Ctl.Flow.current) {

					C.Ctl.Flow.history.unshiftObject(C.Mdl.Run.create({
						"runId": response.params.id,
						"endStatus": "STOPPED",
						"startTime": thisFlow.get('lastStarted'),
						"endTime": new Date().getTime() / 1000
					}));
					C.Ctl.Flow.history.popObject();

					C.Ctl.Flow.stop_spin();
				}

				thisFlow.set('runs', thisFlow.runs + 1);
				thisFlow.set('lastStopped', new Date().getTime() / 1000);

			});

		}
	});
});