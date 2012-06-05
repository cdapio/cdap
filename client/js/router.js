
define([], function () {

	return function (Views) {

		return Ember.RouteManager.create({

			rootElement: '#content-body',
			enableLogging: true,
			
			flows: Em.ViewState.create({
				view: Views.Flows,
				enter: function (stateManager, transition) {
					this._super(stateManager, transition);
					App.Controllers.Flows.load();
				}
			}),
			flow: Em.ViewState.create({
				route: 'apps/:aid/flows/:id',
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
			}),
			history: Em.ViewState.create({
				route: 'flows/:id/'
			}),
			logs: Em.ViewState.create({
				route: 'logs/:id',
				view: Views.Logs
			}),
			upload: Em.ViewState.create({
				route: 'upload/:id',
				view: Views.Upload
			})
		});
	};
});