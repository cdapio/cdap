//
// List Controller
//

define([], function () {

	return Em.ArrayProxy.create({
		content: [],
		getObjects: function (type) {

			var self = this;

			//
			// FYI: If type is "Flow" the server auto-subscribes to status changes.
			//
			C.get('manager', {
				method: 'get' + type + 's'
			}, function (error, response) {

				if (error) {
					C.interstitial.label(error);
				} else {
					var objects = response.params;
					var i = objects.length;
					while (i--) {
						self.pushObject(C.Mdl[type].create(objects[i]));
					}
					C.interstitial.hide();

					console.log('calling');
					C.Ctl.List.getStats();

				}

			});

		},

		getStats: function () {

			var content = this.get('content');
			var app, id;

			for (var i = 0; i < content.length; i ++) {

				var app = content[i].get('applicationId');
				var id = content[i].get('flowId');

				var start = Math.round(new Date().getTime() / 1000) - 30;
				var end = Math.round(new Date().getTime() / 1000);

				C.get('monitor', {
					method: 'getTimeSeries',
					params: [app, id, ['processed.count'], start, end]
				}, function (error, response, params) {

					if (C.router.currentState.name !== "flows") {
						return;
					}

					if (!response.params) {
						console.debug('No response for timeseries');
						return;
					}
					var data = response.params.points['processed.count'];

					if (!data) {
						console.debug('No timeseries data!');
						return;
					}

					var k = data.length;
					while(k --) {
						data[k] = data[k].value;
					}

					var c = C.Ctl.List.content;
					var j = c.length;
					while(j--) {

						if(c[j].get('flowId') === params) {
							c[j].set('ts', {'processed.count': data});
							c[j].set('lastProcessed', data[data.length-1]);
							c[j].set('instances', 3);
						}
					}

				}, id);

			}

			setTimeout(function () {
				C.Ctl.List.getStats();
			}, 1000);

		},

		unload: function () {
			this.set('content', []);
		}

	});

});