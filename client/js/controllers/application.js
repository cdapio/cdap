
define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		load: function (id) {

			this.clear();

			App.socket.request('rest', {
				method: 'applications',
				params: id
			}, function (response) {
				var flows = response.params.flows;
				console.log(flows.length);
				for (var i = 0; i < flows.length; i ++) {
					flows[i] = App.Models.Flow.create(flows[i]);
					App.Controllers.Application.pushObject(flows[i]);
				}
			});
		}

	});

});