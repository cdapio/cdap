
define([], function () {

	return function (Views) {

		return Ember.RouteManager.create({

			enableLogging: true,

			todos: Em.ViewState.create({
				view: Views.List,
				home: Em.State.create({

				})
			})
		});
	};
});