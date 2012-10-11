//
// List Controller
//

define([], function () {

	return Em.Object.create({
		types: Em.Object.create(),
		getObjects: function (type, callback) {

			var self = this;
			this.set('entityType', type);

			//** Hax: Remove special case for Flow when ready **//
			
			C.get(type === 'Flow' ? 'manager' : 'metadata', {
				method: 'get' + type + 's',
				params: []
			}, function (error, response, params) {

				if (error) {
					if (typeof callback === 'function') {
						callback([]);
					} else {
						C.interstitial.label(error);
					}
				} else {
					var objects = response.params;
					var i = objects.length, type = params[0];
					while (i--) {
						objects[i] = C.Mdl[type].create(objects[i]);
					}
					if (typeof params[1] === 'function') { // For you
						callback(objects);

					} else { // For me
						self.set('types.' + type, Em.ArrayProxy.create({content: objects}));
						C.interstitial.hide();
						C.Ctl.List.getStats();
					}
				}
			}, [type, callback]);
		},

		__timeout: null,
		getStats: function () {

			var objects, content;

			if ((objects = this.get('types.' + this.get('entityType')))) {

				content = objects.get('content');

				for (var i = 0; i < content.length; i ++) {
					if (typeof content[i].getUpdateRequest === 'function') {
						C.get.apply(C, content[i].getUpdateRequest());
					}
				}

				this.__timeout = setTimeout(function () {
					C.Ctl.List.getStats();
				}, 1000);

			}

		},

		viewType: function () {

			return Em.get('C.Vw.' + this.get('entityType') + 'List');

		}.property().cacheable(false),

		unload: function () {
			clearTimeout(this.__timeout);
			this.set('types', Em.Object.create());
		}

	});

});