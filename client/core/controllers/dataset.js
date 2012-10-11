//
// DataSet Controller
//

define([], function () {
	
	return Em.ArrayProxy.create({

		load: function (id) {

			var self = this;

			C.get('metadata', {
				method: 'getDataset',
				params: ['Dataset', {
					id: id
				}]
			}, function (error, response) {

				self.set('current', C.Mdl.Dataset.create(response.params));
				C.interstitial.hide();
				
			});

		},

		unload: function () {

		}

	});

});