//
// App Status Controller
//

define([], function () {

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),

		load: function () {

			var self = this;
			this.__remain = 3;

			var model = this.get('model');

			self.set('elements.Flow', Em.ArrayProxy.create({content: []}));
			self.set('elements.Stream', Em.ArrayProxy.create({content: []}));
			self.set('elements.Procedure', Em.ArrayProxy.create({content: []}));
			self.set('elements.Dataset', Em.ArrayProxy.create({content: []}));

			C.Api.getElements('Flow', function (objects) {
				if (!self.get('elements.Flow')) {
					self.set('elements.Flow', Em.ArrayProxy.create({content: []}));
				}

				// ** HAX ** //
				var i = objects.length;
				var flows = [], procedures = [];
				while(i--) {
					if (objects[i].type === 1) {
						procedures.push(objects[i]);
					} else {
						flows.push(objects[i]);
					}
				}

				self.get('elements.Flow').pushObjects(flows);
				self.__loaded();

			}, model.id);

			C.Api.getElements('Procedure', function (objects) {
				if (!self.get('elements.Procedure')) {
					self.set('elements.Procedure', Em.ArrayProxy.create({content: []}));
				}
				self.get('elements.Procedure').pushObjects(objects);
				self.__loaded();
			}, model.id);

			C.Api.getElements('Stream', function (objects) {
				if (!self.get('elements.Stream')) {
					self.set('elements.Stream', Em.ArrayProxy.create({content: []}));
				}
				self.get('elements.Stream').pushObjects(objects);
				self.__loaded();
			}, model.id);

			C.Api.getElements('Dataset', function (objects) {
				if (!self.get('elements.Dataset')) {
					self.set('elements.Dataset', Em.ArrayProxy.create({content: []}));
				}
				self.get('elements.Dataset').pushObjects(objects);
				self.__loaded();
			}, model.id);

		},
		__remain: -1,
		__loaded: function () {

			if (!(--this.__remain)) {

				var self = this;
				/*
				 * Give the chart Embeddables 100ms to configure
				 * themselves before updating.
				 */
				setTimeout(function () {
					self.getStats();
				}, 100);

			}

		},

		unload: function () {

			clearTimeout(this.__timeout);
			this.set('elements', Em.Object.create());

		},

		startAllFlows: function (done) {

			var flows = this.get('elements.Flow').content;
			var flowCount = flows.length;
			var i = flowCount;

			if (!flowCount) {

				done();

			} else {

				while(i--) {

					flows[i].set('currentState', 'STARTING');

					C.socket.request('manager', {
						method: 'start',
						params: [flows[i].application, flows[i].id, -1, 'FLOW']
					}, function (error, response, flow) {

						flow.set('currentState', 'RUNNING');

						if (!--flowCount) {

							done();

						}

					}, flows[i]);
				}
			}

		},

		stopAllFlows: function (done) {

			var flows = this.get('elements.Flow').content;
			var flowCount = flows.length;
			var i = flowCount;

			if (!flowCount) {

				done();

			} else {

				var requested = false;

				while(i--) {

					if (flows[i].get('currentState') !== 'STOPPED') {

						requested = true;

						flows[i].set('currentState', 'STOPPING');

						C.socket.request('manager', {
							method: 'stop',
							params: [flows[i].application, flows[i].id, -1, 'FLOW']
						}, function (error, response, flow) {

							flow.set('currentState', 'STOPPED');

							if (!--flowCount) {

								done();

							}

						}, flows[i]);
					}
				}

				if (!requested) {
					done();
				}

			}

		},

		startAllProcedures: function (done) {

			var procedures = this.get('elements.Procedure').content;
			var ProcedureCount = procedures.length;
			var i = ProcedureCount;

			if (!ProcedureCount) {

				done();

			} else {

				while(i--) {

					procedures[i].set('currentState', 'STARTING');

					C.socket.request('manager', {
						method: 'start',
						params: [procedures[i].application, procedures[i].id, -1, 'Procedure']
					}, function (error, response, Procedure) {

						Procedure.set('currentState', 'RUNNING');

						if (!--ProcedureCount) {

							done();

						}

					}, procedures[i]);

				}
			}

		},

		stopAllProcedures: function (done) {

			var procedures = this.get('elements.Procedure').content;
			var ProcedureCount = procedures.length;
			var i = ProcedureCount;

			if (!ProcedureCount) {

				done();

			} else {

				while(i--) {

					procedures[i].set('currentState', 'STOPPING');

					C.socket.request('manager', {
						method: 'stop',
						params: [procedures[i].application, procedures[i].id, -1, 'Procedure']
					}, function (error, response, Procedure) {

						Procedure.set('currentState', 'STOPPED');

						if (!--ProcedureCount) {

							done();

						}

					}, procedures[i]);

				}
			}

		},
		__timeout: null,
		getStats: function () {

			var self = this, types = ['Flow', 'Stream', 'Procedure', 'Dataset'];

			if (this.get('model')) {

				C.get.apply(C, this.get('model').getUpdateRequest());

				for (var i = 0; i < types.length; i ++) {

					var content = this.get('elements').get(types[i]).get('content');
					for (var j = 0; j < content.length; j ++) {
						if (typeof content[j].getUpdateRequest === 'function') {
							C.get.apply(C, content[j].getUpdateRequest());
						}
					}
				}


				var storage = 0;
				var streams = this.get('elements.Stream').content;
				for (var i = 0; i < streams.length; i ++) {
					storage += streams[i].get('storage');
				}
				var datasets = this.get('elements.Dataset').content;
				for (var i = 0; i < datasets.length; i ++) {
					storage += datasets[i].get('storage');
				}

				self.get('model').set('storageLabel', C.Util.bytes(storage)[0]);
				self.get('model').set('storageUnits', C.Util.bytes(storage)[1]);

			}

			this.__timeout = setTimeout(function () {
				self.getStats();
			}, 1000);

		},

		/*
		 * Application maintenance features
		 */

		delete: function () {

			C.Modal.show(
				"Delete Application",
				"Are you sure you would like to delete this Application? This action is not reversible.",
				$.proxy(function (event) {

					var app = this.get('model');

					C.get('metadata', {
						method: 'deleteApplication',
						params: ['Application', {
							id: app.id
						}]
					}, function (error, response) {

						C.Modal.hide(function () {

							if (error) {
								C.Modal.show('Error Deleting', error.message);
							} else {
								window.history.go(-1);
							}

						});

					});
				}, this));

		},

		/*
		 * Application promotion features
		 */

		promotePrompt: function () {

			var view = Em.View.create({
				controller: this,
				model: this.get('model'),
				templateName: 'promote',
				classNames: ['popup-modal', 'popup-full'],
				credentialBinding: 'C.Env.credential'
			});

			view.append();
			this.promoteReload();

		},

		promoteReload: function () {

			this.set('loading', true);

			var self = this;
			self.set('destinations', []);
			self.set('message', null);
			self.set('network', false);

			$.post('/credential', 'apiKey=' + C.Env.get('credential'),
				function (result, status) {

				$.getJSON('/destinations', function (result, status) {

					if (result === 'network') {

						self.set('network', true);

					} else {

						var destinations = [];

						for (var i = 0; i < result.length; i ++) {

							destinations.push({
								id: result[i].vpc_name,
								name: result[i].vpc_label + ' (' + result[i].vpc_name + '.continuuity.net)'
							});

						}

						self.set('destinations', destinations);

					}

					self.set('loading', false);

				});

			});

		}.observes('C.Env.credential'),

		promoteSubmit: function () {

			this.set("pushing", true);
			var model = this.get('model');
			var self = this;

			var destination = self.get('destination');
			if (!destination) {
				return;
			}

			destination += '.continuuity.net';

			C.get('far', {
				method: 'promote',
				params: [model.id, destination, C.Env.get('credential')]
			}, function (error, response) {

				if (error) {

					self.set('finished', 'Error');
					if (error.name) {
						self.set('finishedMessage', error.name + ': ' + error.message);
					} else {
						self.set('finishedMessage', response.message || JSON.stringify(error));
					}

				} else {

					self.set('finished', 'Success');
					self.set('finishedMessage', 'Successfully pushed to ' + destination + '.');
					self.set('finishedLink', 'https://' + destination + '/' + window.location.hash);
				}

				self.set("pushing", false);

			});

		}

	});

	Controller.reopenClass({
		type: 'App',
		kind: 'Controller'
	});

	return Controller;

});