//
// Flow Controller. 
//

define([], function () {

	return Em.ArrayProxy.create({
		content: [],
		types: Em.Object.create(),
		get_flowlet: function (id) {
			id = id + "";
			var content = this.types.Flowlet.content;
			for (var k = 0; k < content.length; k++) {
				if (content[k].name === id) {
					return content[k];
				}
			}
			content = this.types.Stream.content;
			for (k = 0; k < content.length; k++) {
				if (content[k].name === id) {
					return content[k];
				}
			}
		},

		unload: function () {

			this.get('content').clear();
			this.set('content', []);

			this.set('types.Flowlet', Em.Object.create());
			this.set('current', null);

		},

		__currentFlowletLabel: 'processed.count',

		load: function (app, id) {

			var self = this;

			C.interstitial.loading();
			C.get('manager', {
				method: 'getFlowDefinition',
				params: [app, id]
			}, function (error, response) {

				self.set('current', C.Mdl.Flow.create(response.params));

				var flowlets = response.params.flowlets;
				var objects = [];
				for (var i = 0; i < flowlets.length; i ++) {
					objects.push(C.Mdl.Flowlet.create(flowlets[i]));
				}

				self.set('types.Stream', Em.ArrayProxy.create());
				self.set('types.Flowlet', Em.ArrayProxy.create({content: objects}));
				
				C.router.applicationController.view.visualizer.drawGraph();


				C.get('manager', {
					method: 'getFlowHistory',
					params: [app, id]
				}, function (error, response) {

					var history = response.params;

					for (var i = 0; i < history.length; i ++) {

						self.pushObject(C.Mdl.Run.create(history[i]));

					}

					C.interstitial.hide();

				});
			});
		},
		loadRun: function (runId) {

			var app = this.get('current').app;
			var id = this.get('current').id;
			var self = this;

			C.get('monitor', {
				method: 'getCounters',
				params: [app, id, runId]
			}, function (error, response) {

				var metrics = response.params;
				var i = metrics.length;
				while (i--) {

					if (metrics[i].name === 'processed.count') {

						self.get_flowlet(metrics[i].qualifier).set('label', C.util.number(metrics[i].value));

					}

				}

				$('#flowviz-container').removeClass('flowviz-fade');

			});

		}
	});
});