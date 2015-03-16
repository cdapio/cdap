/*
 * Flowlet Controller
 */

define([], function () {

	var Controller = Ember.Controller.extend({

		elements: Em.Object.create(),

		/*
		 * This syntax makes the FlowStatus controller available to this controller.
		 * This allows us to access the flow model that has already been loaded.
		 */
		needs: ['FlowStatus'],

		load: function () {
			this.clearTriggers(true);
			/*
			 * The FlowStatus controller has already loaded the flow.
			 * The flow that has been loaded has the flowlet model we need.
			 */
			var flow = this.get('controllers').get('FlowStatus').get('model');

			var model = this.get('model');
			model = this.get('controllers.FlowStatus.elements.Flowlet').find(function (item) {
				if (item.get('id') === model.get('id')) {
					return true;
				}
			});
			this.set('model', model);

			/*
			 * Track container metric.
			 */
			model.trackMetric('/system/' + model.get('context') + '/resources.used.containers', 'currents', 'containers', 'step');

			/*
			 * Setup connections based on the Flow.
			 */
			var cx = flow.connections;
			function findContributors(direction, flowlet, input) {
				var res = [];
				var opp = 'from';
				if (direction === 'from') {
					opp = 'to';
				}

				for (var i = 0; i < cx.length; i ++) {
					if (cx[i][direction]['flowlet'] === flowlet) {
						res.push({name: cx[i][opp]['flowlet'] || cx[i][opp]['stream']});
					}
				}
				return res;
			}
			var streams = flow.flowletStreams.filter(function (stream) {
				return stream.name === model.name;
			})[0];
			var inputs = [], outputs = [];

			/*
			 * Find inputs and outputs.
			 */
			for (var i in streams) {
				if (streams[i].second === 'IN') {
					inputs.push({
						'app': flow.get('app'),
						'flow': flow.get('name'),
						'flowlet': model.get('id'),
						'id': i,
						'contrib': findContributors('to', model.name, i)
					});
				} else if (streams[i].second === 'OUT') {
					outputs.push({
						'app': flow.get('app'),
						'flow': flow.get('name'),
						'flowlet': model.get('id'),
						'id': i,
						'contrib': findContributors('from', model.name, i)
					});
				}
			}
			this.get('model').set('inputs', inputs);
			this.get('model').set('outputs', outputs);

			/*
			 * Newness
			 */
			var queues = [], stream = null;
			this.set('elements.Queue', Em.ArrayProxy.create({content: []}));

			for (var id in streams) {
				streams[id].id = id;
				streams[id].flowlet = model.get('id');
				streams[id].app = flow.get('app');
				streams[id].flow = flow.get('name');
				queues.push(C.Queue.create(streams[id]));
			}
			this.get('elements.Queue').pushObjects(queues);

			/*
			 * Select the Inputs tab.
			 */
			this.select('inputs');

			/*
			 * Give the chart Embeddables 100ms to configure
			 * themselves before updating.
			 */
			var self = this;
			setTimeout(function () {
				self.getStats();
			}, C.EMBEDDABLE_DELAY);

		},

		unload: function () {
			clearTimeout(this.__timeout);
		},

		ajaxCompleted: function () {
			return this.get('timeseriesCompleted');
		},

		clearTriggers: function (value) {
			this.set('timeseriesCompleted', value);
		},

		getStats: function (self) {
			if (!this.ajaxCompleted()) {
				return;
			}
			var models = [this.get('model')];

			C.Util.updateCurrents(models, this.HTTP, this, C.RESOURCE_METRICS_BUFFER);

			models = models.concat(this.get('elements.Queue').content);
			this.clearTriggers(false);
			C.Util.updateTimeSeries(models, this.HTTP, this);

			var self = this;
			self.__timeout = setTimeout(function () {
				self.getStats(self);
			}, C.POLLING_INTERVAL);

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

			var model = this.get('controllers').get('FlowStatus').get('model');

			/*
			 * HAX: The URL route needs the ID of a flow to be app_id:flow_id.
			 * However, Ember is smart enough to not reload the parent controller.
			 * Therefore, the "correct" ID is preserved on the parent controller's model.
			 */

			if (model.id && model.id.indexOf(':') === -1) {
				model.id = model.app + ':' + model.id;
			}

			this.transitionToRoute('FlowStatus', model);

		},

		navigate: function (flowletName) {

			var model = this.get('controllers.FlowStatus').get_flowlet(flowletName);
			if (model.level === 'stream') {
				this.transitionToRoute('FlowStatus.Stream', model);
			} else {
				this.transitionToRoute('FlowStatus.Flowlet', model);
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


			var self = this;
			C.Modal.show(
				"Flowlet Instances",
				'Change instances to ' + input + ' for ' + '"' + this.get('model').name + '" flowlet?',
				function () {
					self.addInstances(input, function () {
					});
				});
    },

		addInstances: function (instancesRequested, done) {

			var flow = this.get('controllers').get('FlowStatus').get('model');
			var model = this.get('model');

      var app = flow.get('app');
      var flow = flow.get('name');
      var flowlet = model.name;

      this.HTTP.put('rest', 'apps', app, 'flows', flow, 'flowlets', flowlet, 'instances', {
        data: '{"instances":' + instancesRequested + '}'
      }, function (response) {

        if (response.error) {
          C.Modal.show('Container Error', response.error);
        } else {
          model.set('instances', instancesRequested);
        }

        if(typeof done === 'function'){
          done();
        }

      });
		},
	});

	Controller.reopenClass({
		type: 'FlowStatusFlowlet',
		kind: 'Controller'
	});

	return Controller;

});
