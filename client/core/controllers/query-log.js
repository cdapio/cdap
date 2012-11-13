//
// Flow Log Controller.
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

		__currentFlowletLabel: 'processed.count',

		load: function (app, id) {

			var self = this;

			C.interstitial.loading();
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

			});

			function logInterval () {

				if (C.router.currentState.get('path') !== 'root.queries.log') {
					clearInterval(self.interval);
					return;
				}

				C.get('monitor', {
					method: 'getLog',
					params: [app, id, 1024 * 10]
				}, function (error, response) {

					if (C.router.currentState.get('path') !== 'root.queries.log') {
						clearInterval(self.interval);
						return;
					}

					if (C.router.currentState.name !== 'log') {
						return;
					}

					if (error) {

						C.router.applicationController.view.set('responseBody', JSON.stringify(error));

					} else {

						var items = response.params;
						C.router.applicationController.view.set('responseBody', items.join('\n'));
					}
					
					var textarea = C.router.applicationController.view.get('logView').get('element');

					setTimeout(function () {
						textarea = $(textarea);
						textarea.scrollTop(textarea[0].scrollHeight - textarea.height());
					}, 200);

					C.interstitial.hide();

				});

			}

			logInterval();

			this.interval = setInterval(logInterval, 1000);

		},

		interval: null,
		unload: function () {

			this.get('content').clear();
			this.set('content', []);
			this.set('current', null);
			clearInterval(this.interval);

		}

	});
});