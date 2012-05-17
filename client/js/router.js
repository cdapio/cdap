
define([], function () {

	return function (Views) {

		return Ember.RouteManager.create({

			rootElement: '#content-body',
			enableLogging: true,
			initialState: 'dashboard',
			dashboard: Em.ViewState.create({
				route: '/',
				enter: function (stateManager, transition) {
					$('#content-header').html('Dashboard');
				}
			}),
			flows: Em.ViewState.create({
				route: 'flows',
				view: Views.Flows,
				enter: function (stateManager, transition) {
					this._super(stateManager, transition);
					$('#content-header').html('Flows');
					App.Controllers.Flows.load();
				},
			}),
			flow: Em.ViewState.create({
				route: 'flows/:id',
				view: Views.Flow,
				enter: function(stateManager, transition) {
					this._super(stateManager, transition);
					var params = stateManager.get('params');
					var id = params.id;
					App.Controllers.Flow.load(id);
				}
			})
		});
	};
});