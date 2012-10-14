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
				self.get('types.Query').pushObjects(queries);

				// self.get('types.Flow').pushObjects(objects);
				self.__loaded();

			});

			C.Ctl.List.getObjects('Stream', function (objects) {
				if (!self.get('types.Stream')) {
					self.set('types.Stream', Em.ArrayProxy.create({content: []}));
				}
				self.get('types.Stream').pushObjects(objects);
				self.__loaded();
			});

			C.Ctl.List.getObjects('Dataset', function (objects) {
				if (!self.get('types.Dataset')) {
					self.set('types.Dataset', Em.ArrayProxy.create({content: []}));
				}
				self.get('types.Dataset').pushObjects(objects);
				self.__loaded();
			});

		},
		__remain: -1,
		__loaded: function () {

			if (!(--this.__remain)) {
				C.interstitial.hide();
				this.getStats();
			}

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