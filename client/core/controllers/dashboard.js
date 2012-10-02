//
// Dashboard Controller
//

define([], function () {
	
	return Em.ArrayProxy.create({
		types: Em.Object.create(),
		load: function () {

			var self = this;

			self.set('types.App', Em.ArrayProxy.create({content: []}));

			C.Ctl.List.getObjects('App', function (objects) {
				var i = objects.length;
				while (i--) {
					objects[i] = C.Mdl['App'].create(objects[i]);
				}
				self.get('types.App').pushObjects(objects);
				self.getStats();

				C.interstitial.hide();
			});
		},

		__timeout: null,
		getStats: function () {

			var self = this;

			if ((objects = this.get('types.App'))) {

				content = objects.get('content');

				for (var i = 0; i < content.length; i ++) {
					if (typeof content[i].getUpdateRequest === 'function') {
						C.get.apply(C, content[i].getUpdateRequest());
					}
				}

				self.__timeout = setTimeout(function () {
					self.getStats();
				}, 1000);
			}

		},

		unload: function () {
			clearTimeout(this.__timeout);
			this.set('types', Em.Object.create());
		}
	});
});