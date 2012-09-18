
require.config({
	paths: {
		"core": "../core/"
	}
});

define (['core/app', 'patch/views/index'], function (App, ViewPatch) {

	App.Context = {
		Views: ViewPatch
	};

	$.extend(App, {
		Router: Ember.Router.extend({
			enableLogging: true,
			root: Ember.Route.extend({
				home: Ember.Route.extend({
					route: '/',
					connectOutlets: function (router, params) {
						App.Controllers.Flows.load();
						router.get('applicationController').connectOutlet({
							viewClass: App.Views.Flows,
							controller: App.Controllers.Flows
						});
					}
				}),
				upload: Ember.Route.extend({
					route: '/upload',
					connectOutlets: function (router, event) {
						router.get('applicationController').connectOutlet({
							viewClass: App.Views.Upload,
							controller: App.Controllers.Upload
						});
					}
				}),
				flows: Ember.Route.extend({
					route: '/flows',
					index: Ember.Route.extend({
						route: '/',
						connectOutlets: function (router, params) {
							App.Controllers.Flows.load();
							router.get('applicationController').connectOutlet({
								viewClass: App.Views.Flows,
								controller: App.Controllers.Flows
							});
						}
					}),
					flow: Ember.Route.extend({
						route: '/:appId/:flowId',
						connectOutlets: function (router, params) {
							App.Controllers.Flow.load(params.appId, params.flowId);
							router.get('applicationController').connectOutlet({
								viewClass: App.Views.Flow,
								controller: App.Controllers.Flow
							});
						}
					})
				})
			})
		})
	});

	$(function () {
		App.initialize();
	});
});