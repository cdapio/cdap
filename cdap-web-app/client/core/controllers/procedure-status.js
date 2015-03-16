/*
 * Procedure Controller
 */

define([], function () {

	var Controller = Ember.Controller.extend({

		needs: ['Procedure'],

		load: function () {

			this.clearTriggers(true);

			var model = this.get('model');
			var self = this;

			var controller = this.get('controllers.Procedure');

			if (controller.get('previousProcedure') !== model.get('id')) {
				controller.set('requestMethod', '');
				controller.set('requestParams', '');
				controller.set('responseBody', '');
				controller.set('previousProcedure', model.get('id'));
			}

			this.interval = setInterval(function () {
				self.updateStats();
			}, C.POLLING_INTERVAL);

			/*
			 * Give the chart Embeddables 100ms to configure
			 * themselves before updating.
			 */
			setTimeout(function () {
				self.updateStats();
			}, C.EMBEDDABLE_DELAY);

		},

		unload: function () {

			clearInterval(this.interval);

		},

		ajaxCompleted: function () {
			return this.get('timeseriesCompleted') && this.get('aggregatesCompleted');
		},

		clearTriggers: function (value) {
			this.set('timeseriesCompleted', value);
			this.set('aggregatesCompleted', value);
		},

		updateStats: function () {
			if (!this.ajaxCompleted()) {
				return;
			}
			this.get('model').updateState(this.HTTP);
			this.clearTriggers(false);
			C.Util.updateTimeSeries([this.get('model')], this.HTTP, this);
			C.Util.updateAggregates([this.get('model')], this.HTTP, this);

			C.Util.updateCurrents([this.get('model')], this.HTTP, this, C.RESOURCE_METRICS_BUFFER);

			var appId = this.get('model.app');
			var procedureName = this.get('model.name');
			var self = this;

			this.HTTP.get('rest', 'apps', appId, 'procedures', procedureName, 'instances', function (response) {

				self.set('model.instances', response.instances);

			});

		},

		/**
		 * Action handlers from the View
		 */
		exec: function () {

			var model = this.get('model');
			var action = model.get('defaultAction');
			if (action && action.toLowerCase() in model) {
				model[action.toLowerCase()](this.HTTP);
			}

		},

    keyPressed: function (evt) {
      var btn = this.$().next();
      var inp = this.value;
      return C.Util.handleInstancesKeyPress(btn, inp, this.placeholder);
    },

    changeInstances: function () {
      var inputStr = this.get('instancesInput');
      var input = parseInt(inputStr);

      this.set('instancesInput', '');
      setTimeout(function () {
        $('#instancesInput').keyup();
      },500);

      if (this.get('model') && this.get('model').instances === input) {
        return; //no-op
      }

      this.setInstances(input);
    },

		setInstances: function (instances) {

			var self = this;
			var appId = this.get('model.app');
			var procedureName = this.get('model.name');

			C.Modal.show(
				"Procedure Instances",
				"Change Instances to " + instances + " for " + ' "' + procedureName + '" procedure?',
				function () {
//
					self.HTTP.put('rest', 'apps', appId, 'procedures', procedureName, 'instances', {
						data: '{"instances": ' + instances + '}'
					}, function (response) {

						self.set('model.instances', instances);

					});
				});

		},

    actualInstances: function () {
      if (C.get('isLocal')) {
        return this.get('model.instances');
      } else {
        var instances = this.get('model.containersLabel');
        return instances + ' instance' + (instances === 1 ? '' : 's');
      }
    }.property('model.instances','model.containersLabel'),

		config: function () {

			var self = this;
			var model = this.get('model');

			this.transitionToRoute('ProcedureStatus.Config');

		},

		responseBody: null,
		responseCode: null,

		submit: function (event) {

			var self = this;
			var controller = this.get('controllers.Procedure');

			var appId = this.get('model').applicationId;
			var procedureName = this.get('model').serviceName;
			var methodName = controller.get('requestMethod');

			this.HTTP.post('rest', 'apps', appId, 'procedures', procedureName, 'methods', methodName, {
					data: controller.get('requestParams')
				}, function (response) {

				if (response) {
					if (typeof(response) === 'object') {
						self.set('responseBody', JSON.stringify(response, undefined, 2) || '[ No content ]');
					} else {
						self.set('responseBody', response || '[ No content ]');
					}

				} else {
					self.set('responseBody', '[ No response received ]');
				}

			});

		}

	});

	Controller.reopenClass({
		type: 'ProcedureStatus',
		kind: 'Controller'
	});

	return Controller;

});
