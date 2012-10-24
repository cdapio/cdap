//
// Query Controller.
//

define([], function () {

	return Em.Object.create({
		current: null,
		types: Em.Object.create(),
		load: function (app, id) {

			var self = this;

			C.get('metadata', {
				method: 'getQuery',
				params: ['Query', {
					application: app,
					id: id
				}]
			}, function (error, response) {

				response.params.currentState = 'UNKNOWN';
				response.params.version = -1;
				response.params.type = 'Query';
				response.params.applicationId = app;

				self.set('current', C.Mdl.Query.create(response.params));

				//
				// Request Flow Status
				//
				C.get('manager', {
					method: 'status',
					params: [app, id, -1]
				}, function (error, response) {

					if (response.params) {
						self.get('current').set('currentState', response.params.status);
						self.get('current').set('version', response.params.version);
						C.interstitial.hide();
						self.startStats();
					}

					self.interval = setInterval(function () {
						self.refresh();
					}, 1000);

				});

			});
		},

		startStats: function () {
			var self = this;
			clearTimeout(this.updateTimeout);
			this.updateTimeout = setTimeout(function () {
				self.updateStats();
			}, 1000);
		},

		updateStats: function () {
			var self = this;

			if (!this.get('current')) {
				self.startStats();
				return;
			}

			// Update timeseries data for current flow.
			C.get.apply(C, this.get('current').getUpdateRequest());

			this.startStats();

		},

		unload: function () {

			clearTimeout(this.updateTimeout);
			this.set('current', null);
			clearInterval(this.interval);

		},

		refresh: function () {

			var self = this;
			var app = this.get('current').applicationId;
			var id = this.get('current').id;

			if (this.__pending) {
				return;
			}

			C.get('manager', {
				method: 'status',
				params: [app, id, -1]
			}, function (error, response) {

				if (response.params && self.get('current')) {
					self.get('current').set('currentState', response.params.status);
				}
			});
		},

		start: function (app, id, version) {

			$('#flow-alert').hide();

			var thisQuery = C.Ctl.Query.current;
			var self = this;

			self.__pending = true;
			thisQuery.set('currentState', 'STARTING');

			C.socket.request('manager', {
				method: 'start',
				params: [app, id, -1, 'QUERY']
			}, function (error, response) {

				self.__pending = false;

				thisQuery.set('lastStarted', new Date().getTime() / 1000);

				if (C.Ctl.Query.current) {
					C.Ctl.Query.updateStats();
				}

			});

		},
		stop: function (app, id, version) {

			$('#flow-alert').hide();

			var thisQuery = C.Ctl.Query.current;
			var self = this;
			
			self.__pending = true;
			thisQuery.set('currentState', 'STOPPING');

			C.socket.request('manager', {
				method: 'stop',
				params: [app, id, -1, 'QUERY']
			}, function (error, response) {

				self.__pending = false;

			});

		}

	});
});