//
// Stream Controller
//

define([], function () {
	
	return Em.ArrayProxy.create({

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
				
			});

		},

		unload: function () {

		}

	});

});