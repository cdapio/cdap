//
// App Status Controller
//

define([], function () {
	
	return Em.Object.create({
		types: Em.Object.create(),
		load: function (app) {

			var self = this;
			this.__remain = 3;

			C.get('metadata', {
				method: 'getApplication',
				params: ['Application', {
					id: app
				}]
			}, function (error, response) {

				if (error) {
					C.interstitial.label(error);
					return;
				}

				self.set('current', C.Mdl.Application.create(response.params));

			});

			self.set('types.Flow', Em.ArrayProxy.create({content: []}));
			self.set('types.Stream', Em.ArrayProxy.create({content: []}));
			self.set('types.Query', Em.ArrayProxy.create({content: []}));
			self.set('types.Dataset', Em.ArrayProxy.create({content: []}));

			C.Ctl.List.getObjects('Flow', function (objects) {
				if (!self.get('types.Flow')) {
					self.set('types.Flow', Em.ArrayProxy.create({content: []}));
				}

				// ** HAX ** //
				var i = objects.length;
				var flows = [], queries = [];
				while(i--) {
					if (objects[i].type === 1) {
						queries.push(objects[i]);
					} else {
						flows.push(objects[i]);
					}
				}

				self.get('types.Flow').pushObjects(flows);
				self.__loaded();

			}, app);

			C.Ctl.List.getObjects('Query', function (objects) {
				if (!self.get('types.Query')) {
					self.set('types.Query', Em.ArrayProxy.create({content: []}));
				}
				self.get('types.Query').pushObjects(objects);
				self.__loaded();
			}, app);

			C.Ctl.List.getObjects('Stream', function (objects) {
				if (!self.get('types.Stream')) {
					self.set('types.Stream', Em.ArrayProxy.create({content: []}));
				}
				self.get('types.Stream').pushObjects(objects);
				self.__loaded();
			}, app);

			C.Ctl.List.getObjects('Dataset', function (objects) {
				if (!self.get('types.Dataset')) {
					self.set('types.Dataset', Em.ArrayProxy.create({content: []}));
				}
				self.get('types.Dataset').pushObjects(objects);
				self.__loaded();
			}, app);

		},
		__remain: -1,
		__loaded: function () {

			if (!(--this.__remain)) {
				C.interstitial.hide();
				this.getStats();
			}

		},

		startAllFlows: function () {

			var flows = this.get('types.Flow').content;
			var flowCount = flows.length;
			var i = flowCount;

			if (!flowCount) {

				$('#start-all-button').find('button').show().attr('disabled', true);
				$('#start-all-button').find('img').hide();

			} else {

				while(i--) {

					flows[i].set('currentState', 'STARTING');

					C.socket.request('manager', {
						method: 'start',
						params: [flows[i].application, flows[i].id, -1, 'FLOW']
					}, function (error, response, flow) {

						flow.set('currentState', 'RUNNING');

						if (!--flowCount) {

							$('#start-all-button').find('button').show().attr('disabled', true);
							$('#start-all-button').find('img').hide();

						}

					}, flows[i]);
				}
			}

		},

		startAll: function () {

			var queries = this.get('types.Query').content;
			var queryCount = queries.length;
			var i = queryCount;

			if (!queryCount) {

				C.Ctl.Application.startAllFlows();

			} else {

				while(i--) {

					queries[i].set('currentState', 'STARTING');

					C.socket.request('manager', {
						method: 'start',
						params: [queries[i].application, queries[i].id, -1, 'QUERY']
					}, function (error, response, query) {
						
						query.set('currentState', 'RUNNING');

						if (!--queryCount) {

							C.Ctl.Application.startAllFlows();

						}

					}, queries[i]);

				}
			}

		},

		stopAll: function () {

			C.get('manager', {
				method: 'stopAll',
				params: []
			}, function (error, response) {

				console.log(arguments);

			});

		},

		__timeout: null,
		getStats: function () {

			var self = this, types = ['Flow', 'Stream', 'Query', 'Dataset'];

			if (this.get('current')) {

				C.get.apply(C, this.get('current').getUpdateRequest());
				
				for (var i = 0; i < types.length; i ++) {

					var content = this.get('types').get(types[i]).get('content');
					for (var j = 0; j < content.length; j ++) {
						if (typeof content[j].getUpdateRequest === 'function') {
							C.get.apply(C, content[j].getUpdateRequest());
						}
					}
				}
				

				var storage = 0;
				var streams = this.get('types.Stream').content;
				for (var i = 0; i < streams.length; i ++) {
					storage += streams[i].get('storage');
				}
				var datasets = this.get('types.Dataset').content;
				for (var i = 0; i < datasets.length; i ++) {
					storage += datasets[i].get('storage');
				}

				self.get('current').set('storageLabel', C.util.bytes(storage)[0]);
				self.get('current').set('storageUnits', C.util.bytes(storage)[1]);

			}

			this.__timeout = setTimeout(function () {
				self.getStats();
			}, 1000);

		},

		unload: function () {
			
			clearTimeout(this.__timeout);
			this.set('types', Em.Object.create());

		}
	});

});