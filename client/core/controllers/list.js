//
// List Controller
//

define([], function () {

	return Em.Object.create({
		types: Em.Object.create(),
		__methodNames: {
			'Application': 'getApplications',
			'Flow': 'getFlows',
			'Stream': 'getStreams',
			'Query': 'getFlows',
			'Dataset': 'getDatasets'
		},
		__plurals: {
			'Application': 'Applications',
			'Flow': 'Flows',
			'Stream': 'Streams',
			'Query': 'Queries',
			'Dataset': 'Datasets'	
		},
		title: function () {
			return this.__plurals[this.get('entityType')];
		}.property('entityType'),
		getObjects: function (type, callback) {

			var self = this;
			this.set('entityType', type);

			//** Hax: Remove special case for Flow when ready **//
			
			C.get(type === 'Flow' || type === 'Query' ? 'manager' : 'metadata', {
				method: this.__methodNames[type],
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
						if (objects[i].type === 1) {
							objects[i] = C.Mdl['Query'].create(objects[i]);
						} else {
							objects[i] = C.Mdl[type].create(objects[i]);
						}
					}
					if (typeof params[1] === 'function') { // For you
						callback(objects);

					} else { // For me

						var i = objects.length;
						while (i--) {
							if (type === 'Query' && objects[i].type === 0) {
								objects.splice(i, 1);
							} else if (type === 'Flow' && objects[i].type === 1) {
								objects.splice(i, 1);
							}
						}

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