
define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		load: function () {

			this.clear();

			App.socket.request('rest', {
				method: 'applications'
			}, function (response) {
				var flows = response.params;
				for (var i = 0; i < flows.length; i ++) {
					flows[i] = App.Models.Application.create(flows[i]);
					App.Controllers.Applications.pushObject(flows[i]);
				}
			});
		}

	});

});