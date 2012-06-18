
define([], function () {

	return function (Views) {

		return Ember.RouteManager.create({

			rootElement: '#content-body',
			enableLogging: true,
			
			flows: Em.ViewState.create({
				view: Views.Flows,
				enter: function (stateManager, transition) {
					this._super(stateManager, transition);
					App.interstitial.hide();
					App.Controllers.Flows.load();
				}
			}),

			upload: Em.ViewState.create({
				route: 'upload/:id',
				view: Views.Upload
			}),

			flow: Em.ViewState.create({
				route: 'flow/:app/:id',
				view: Views.Flow,
				enter: function(stateManager, transition) {
					this._super(stateManager, transition);
					var params = stateManager.get('params');
					App.Controllers.Flow.load(params.app, params.id);
				},
				exit: function (stateManager, transition) {
					this._super(stateManager, transition);
					App.Controllers.Flow.unload();
					App.Views.Flowlet.hide();
				}
			}),

			run: Em.ViewState.create({
				route: 'flow/:app/:id/:run',
				view: Views.Flow,
				enter: function(stateManager, transition) {
					this._super(stateManager, transition);
					var params = stateManager.get('params');
					App.Controllers.Flow.load(params.app, params.id, params.run);
				},
				exit: function (stateManager, transition) {
					this._super(stateManager, transition);
					App.Controllers.Flow.unload();
					App.Views.Flowlet.hide();
				}
			})
		});
	};
});