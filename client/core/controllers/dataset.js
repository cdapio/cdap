//
// DataSet Controller
//

define([], function () {
	
	return Em.ArrayProxy.create({

		load: function () {

			C.interstitial.hide();

		},

		unload: function () {

		}

	});

});