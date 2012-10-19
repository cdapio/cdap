//
// Stream Controller
//

define([], function () {

	return Em.ArrayProxy.create({
		typesBinding: 'current.types',
		/*Em.Object.create({
			'StreamFlow':
		}),*/
		load: function (id) {

			var self = this;

			C.get('metadata', {
				method: 'getStream',
				params: ['Stream', {
					id: id
				}]
			}, function (error, response) {

				self.set('current', C.Mdl.Stream.create(response.params));
				C.interstitial.hide();

				self.startStats();

			});

		},

		startStats: function () {
			var self = this;
			clearTimeout(this.updateTimeout);
			this.updateTimeout = setTimeout(function () {
				self.updateStats();
			}, 1000);
		},

		updateStats: function () {
			var self = this;

			if (!this.get('current')) {
				self.startStats();
				return;
			}

			// Update timeseries data for current flow.
			C.get.apply(C, this.get('current').getUpdateRequest());

			this.startStats();

		},

		unload: function () {

		}

	});

});