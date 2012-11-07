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

				response.params.applicationId = app;

				self.set('current', C.Mdl.Flow.create(response.params));

				var flowlets = response.params.flowlets;
				var objects = [];
				for (var i = 0; i < flowlets.length; i ++) {
					objects.push(C.Mdl.Flowlet.create(flowlets[i]));
				}
				self.set('types.Flowlet', Em.ArrayProxy.create({content: objects}));
				
				var streams = response.params.flowStreams;
				objects = [];
				for (var i = 0; i < streams.length; i ++) {
					objects.push(C.Mdl.Stream.create(streams[i]));
				}
				self.set('types.Stream', Em.ArrayProxy.create({content: objects}));

				C.interstitial.hide();

				// READY TO GO


			});
		}
	});
});