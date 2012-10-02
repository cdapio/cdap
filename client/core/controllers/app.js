//
// App Status Controller
//

define([], function () {
	
	return Em.Object.create({
		types: Em.Object.create(),
		load: function (app) {

			var self = this;
			this.__remain = 3;

			C.get('manager', {
				method: 'getApp',
				params: [app]
			}, function (error, response) {
				
				self.set('current', C.Mdl.App.create(response.params));

			});

			self.set('types.Flow', Em.ArrayProxy.create({content: []}));
			self.set('types.Stream', Em.ArrayProxy.create({content: []}));
			self.set('types.DataSet', Em.ArrayProxy.create({content: []}));

			C.Ctl.List.getObjects('Flow', function (objects) {
				if (!self.get('types.Flow')) {
					self.set('types.Flow', Em.ArrayProxy.create({content: []}));
				}
				self.get('types.Flow').pushObjects(objects);
				self.__loaded();

			});

			C.Ctl.List.getObjects('Stream', function (objects) {
				if (!self.get('types.Stream')) {
					self.set('types.Stream', Em.ArrayProxy.create({content: []}));
				}
				self.get('types.Stream').pushObjects(objects);
				self.__loaded();
			});

			C.Ctl.List.getObjects('DataSet', function (objects) {
				if (!self.get('types.DataSet')) {
					self.set('types.DataSet', Em.ArrayProxy.create({content: []}));
				}
				self.get('types.DataSet').pushObjects(objects);
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

			C.get.apply(C, this.get('current').getUpdateRequest());

			var self = this, types = ['Flow', 'Stream', 'DataSet'];
			for (var i = 0; i < types.length; i ++) {

				var content = this.get('types').get(types[i]).get('content');
				for (var j = 0; j < content.length; j ++) {
					if (typeof content[j].getUpdateRequest === 'function') {
						C.get.apply(C, content[j].getUpdateRequest());
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