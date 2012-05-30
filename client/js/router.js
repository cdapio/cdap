
define([], function () {

	return function (Views) {

		return Ember.RouteManager.create({

			rootElement: '#content-body',
			enableLogging: true,
			initialState: 'dashboard',

			dashboard: Em.ViewState.create({
				view: Views.Dashboard
			}),
			flows: Em.ViewState.create({
				route: 'flows',
				view: Views.Flows,
				enter: function (stateManager, transition) {
					this._super(stateManager, transition);
					App.Controllers.Flows.load();
				}
			}),
			flow: Em.ViewState.create({
				route: 'flows/:id',
				view: Views.Flow,
				enter: function(stateManager, transition) {
					this._super(stateManager, transition);
					var params = stateManager.get('params');
					var id = parseInt(params.id, 10);
					if (id) {
						App.Controllers.Flow.load(id);
					} else {
						App.Controllers.Flows.load();
					}
				},
				exit: function (stateManager, transition) {
					this._super(stateManager, transition);

					App.Controllers.Flow.unload();

				}
			})
		});
	};
});