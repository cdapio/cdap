
define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		current: null,

		updateStats: function () {
			App.socket.request('rest', {
				method: 'flows',
				params: this.get('id')
			}, function (response) {

				var flowlets = response.params.flowlets;
				var cached = App.Controllers.Flow.content;
				for (var i = 0; i < flowlets.length; i ++) {

					for (var k = 0; k < cached.length; k ++) {
						if (cached[k].id == flowlets[i].id) {
							cached[k].set('tuplesIn', flowlets[i].tuplesIn);
						}
					}

				}

			});
		},

		load: function (id) {
			
			this.clear();
			this.set('id', id);

			App.socket.request('rest', {
				method: 'flows',
				params: id
			}, function (response) {

				var flow = App.Models.Flow.create(response.params);
				App.Views.Flow.set('current', flow);

				var flowlets = response.params.flowlets;
				
				for (var i = 0; i < flowlets.length; i ++) {
					flowlets[i] = App.Models.Flowlet.create(flowlets[i]);
					App.Controllers.Flow.pushObject(flowlets[i]);
				}

				var history = [];

				for(var i = 0; i < flow.history.length; i ++) {
					history.push(Ember.Object.create(flow.history[i]));
				}


				history = Ember.ArrayProxy.create({content: history});
				App.Controllers.Flow.set('history', history);

				clearInterval(App.Controllers.Flow.interval);
				App.Controllers.Flow.interval = setInterval(function () {
					App.Controllers.Flow.updateStats();
				}, 1000);

				App.Controllers.Flow.set('current', flow);

			});
		},
		start: function () {
			App.socket.request('rest', {
				method: 'flows',
				params: id + '/start'
			}, function (response) {
				var flows = App.Controllers.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].id == id) {
						flows[i].set('status', 'running');
						return;
					}
				}
			});
		},
		stop: function () {
			App.socket.request('rest', {
				method: 'flows',
				params: id + '/stop'
			}, function (response) {
				var flows = App.Controllers.Flows.content;
				for (var i = 0; i < flows.length; i ++) {
					if (flows[i].id == id) {
						flows[i].set('status', 'stopped');
						return;
					}
				}
			});
		}

	});

});