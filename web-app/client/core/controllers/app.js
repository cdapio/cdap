/*
 * App Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),
		__remaining: -1,

		load: function () {

			/*
			 * This is decremented to know when loading is complete.
			 * There are 4 things to load. See __loaded below.
			 */
			this.__remaining = 5;

			this.set('elements.Flow', Em.ArrayProxy.create({content: []}));
			this.set('elements.Mapreduce', Em.ArrayProxy.create({content: []}));
			this.set('elements.Workflow', Em.ArrayProxy.create({content: []}));
			this.set('elements.Stream', Em.ArrayProxy.create({content: []}));
			this.set('elements.Procedure', Em.ArrayProxy.create({content: []}));
			this.set('elements.Dataset', Em.ArrayProxy.create({content: []}));
			this.clearTriggers(true);

			var self = this;
			var model = this.get('model');

			model.trackMetric('/reactor/apps/{id}/store.bytes', 'aggregates', 'storage');

			/*
			 * Load Streams
			 */
			this.HTTP.rest('apps', model.id, 'streams', function (objects) {

				var i = objects.length;
				while (i--) {

					objects[i] = C.Stream.create(objects[i]);

				}
				self.get('elements.Stream').pushObjects(objects);
				self.__loaded();

			});

			/*
			 * Load Flows
			 */
			this.HTTP.rest('apps', model.id, 'flows', function (objects) {

				var i = objects.length;
				while (i--) {
					objects[i] = C.Flow.create(objects[i]);
				}
				self.get('elements.Flow').pushObjects(objects);
				self.__loaded();

			});

      /*
       * Load Mapreduce
       */
      this.HTTP.rest('apps', model.id, 'mapreduce', function (objects) {

          var i = objects.length;
          while (i--) {
              objects[i] = C.Mapreduce.create(objects[i]);
          }
          self.get('elements.Mapreduce').pushObjects(objects);
          self.__loaded();

      });

      /*
       * Load Workflows
       */
      this.HTTP.rest('apps', model.id, 'workflows', function (objects) {

          var i = objects.length;
          while (i--) {
              objects[i] = C.Workflow.create(objects[i]);
          }
          self.get('elements.Workflow').pushObjects(objects);
          self.__loaded();

      });

			/*
			 * Load Datasets
			 */
			this.HTTP.rest('apps', model.id, 'datasets', function (objects) {

				var i = objects.length;
				while (i--) {
					objects[i] = C.Dataset.create(objects[i]);
				}
				self.get('elements.Dataset').pushObjects(objects);
				self.__loaded();

			});

			/*
			 * Load Procedures
			 */
			this.HTTP.rest('apps', model.id, 'procedures', function (objects) {

				var i = objects.length;
				while (i--) {
					objects[i] = C.Procedure.create(objects[i]);
				}
				self.get('elements.Procedure').pushObjects(objects);
				self.__loaded();

			});

		},

		__loaded: function () {

			if (!(--this.__remaining)) {

				var self = this;
				/*
				 * Give the chart Embeddables 100ms to configure
				 * themselves before updating.
				 */
				setTimeout(function () {
					self.updateStats();
				}, C.EMBEDDABLE_DELAY);

				this.interval = setInterval(function () {
					self.updateStats();
				}, C.POLLING_INTERVAL);

			}

		},

		unload: function () {

			clearInterval(this.interval);
			this.set('elements', Em.Object.create());

		},

		ajaxCompleted: function () {
			return this.get('timeseriesCompleted') && this.get('aggregatesCompleted');
		},

		clearTriggers: function (value) {
			this.set('timeseriesCompleted', value);
			this.set('aggregatesCompleted', value);
		},

		updateStats: function () {
			if (!this.ajaxCompleted() || C.currentPath !== 'App') {
				return;
			}

			var self = this, types = ['Flow', 'Mapreduce', 'Workflow', 'Stream', 'Procedure', 'Dataset'];

			if (this.get('model')) {

				var i, models = [this.get('model')];
				for (i = 0; i < types.length; i ++) {
					models = models.concat(this.get('elements').get(types[i]).get('content'));
				}

				/*
				 * Hax until we have a pub/sub system for state.
				 */
				i = models.length;
				while (i--) {
					if (typeof models[i].updateState === 'function') {
						models[i].updateState(this.HTTP);
					}
				}
				/*
				 * End hax
				 */
				this.clearTriggers(false);
				// Scans models for timeseries metrics and updates them.
				C.Util.updateTimeSeries(models, this.HTTP, this);

				// Scans models for aggregate metrics and udpates them.
				C.Util.updateAggregates(models, this.HTTP, this);

			}

		},

		hasRunnables: function () {

			var flow = this.get('elements.Flow.content');
			var mapreduce = this.get('elements.Mapreduce.content');
			var procedure = this.get('elements.Procedure.content');

			if (!flow.length && !mapreduce.length && !procedure.length) {
				return false;
			}
			return true;

		}.property('elements.Flow', 'elements.Mapreduce', 'elements.Procedure'),

		transition: function (elements, action, transition, endState, done) {

			var i = elements.length, model, appId = this.get('model.id');
			var remaining = i;

			var HTTP = this.HTTP;

			while (i--) {

				if (elements[i].get('currentState') === transition ||
					elements[i].get('currentState') === endState) {
					remaining --;
					continue;
				}

				var model = elements[i];
				var entityType = model.get('type').toLowerCase() + 's';
				model.set('currentState', transition);
				HTTP.post('rest', 'apps', appId, entityType, model.get('name'), action,
					function (response) {

						model.set('currentState', endState);
						if (!--remaining && typeof done === 'function') {
							done();
						}

				});

			}

		},

		startAll: function (kind) {

			var elements = this.get('elements.' + kind + '.content');

			C.Util.interrupt();
			this.transition(elements, 'start', 'starting', 'running', C.Util.proceed);

		},

		stopAll: function (kind) {

			var elements = this.get('elements.' + kind + '.content');

			C.Util.interrupt();
			this.transition(elements, 'stop', 'stopping', 'stopped', C.Util.proceed);

		},

		/*
		 * Application maintenance features
		 */

		"delete": function () {

			var self = this;

			C.Modal.show(
				"Delete Application",
				"Are you sure you would like to delete this Application? This action is not reversible.",
				$.proxy(function (event) {

					var app = this.get('model');

					C.Util.interrupt();

					this.HTTP.del('rest', 'apps', app.id, function (err, status) {
						if (err !== "") {

							C.Util.proceed(function () {
								setTimeout(function () {
									C.Modal.show("Could not Delete", status);
								}, 500);
							});

						} else {

							C.Util.proceed(function () {
								self.transitionToRoute('index');
							});

						}

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
			if (!self.get('promoteSucceeded')) {
				self.set('finishedMessage', '');
			}

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

			this.HTTP.post('rest', 'apps', model.id, 'promote', {
				hostname: destination,
				apiKey: C.Env.credential
			}, function (response, status, statusText) {

				if (status !== 200) {

					self.set('finished', 'Error');
					if (response) {
						self.set('finishedMessage', response);
					} else {
						self.set('finishedMessage', 'Could not push to server.');
					}

				} else {

					self.set('finished', 'Success');
					self.set('finishedMessage', 'Successfully pushed to ' + destination + '.');
					self.set('finishedLink', 'https://' + destination + '/' + window.location.hash);
				}

				self.set("pushing", false);

			});

		},

		promoteSucceeded: function () {
			return this.get('finished') === 'Success';
		}.observes('finished').property('finished')

	});

	Controller.reopenClass({
		type: 'App',
		kind: 'Controller'
	});

	return Controller;

});
