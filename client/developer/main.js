
require.config({
	paths: {
		"core": "../core/"
	}
});

define (['core/app', 'patch/views/index'], function (C, Patch) {

	C.Ctx = {
		Vw: Patch
	};

	$.extend(C, {
		Router: Ember.Router.extend({
			enableLogging: true,
			root: Ember.Route.extend({
				home: Ember.Route.extend({
					route: '/',
					connectOutlets: function (router, context) {

						router.transitionTo('flows');
						
						/*
						router.get('applicationController').connectOutlet({
							viewClass: C.Vw.Dash,
							controller: C.Ctl.Dash
						});
						*/
					}
				}),
				apps: Ember.Route.extend({
					route: '/apps',
					list: Ember.Route.extend({
						route: '/',
						connectOutlets: function (router, context) {
							C.Ctl.List.getObjects("App");
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.Flows,
								controller: C.Ctl.List
							});
						},
						enter: function () {
							C.interstitial.show();
						},
						navigateAway: function () {
							C.Ctl.List.unload();
						}
					}),
					index: Ember.Route.extend({
						route: '/:appId',
						connectOutlets: function (router, context) {
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.App,
								controller: C.Ctl.App
							});
						},
					}),
					flow: Ember.Route.extend({
						route: '/:appId/:flowId',
						connectOutlets: function (router, context) {
							C.Ctl.Flow.load(context.appId, context.flowId);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.Flow,
								controller: C.Ctl.Flow
							});
						}
					})
				}),
				flows: Ember.Route.extend({
					route: '/flows',
					connectOutlets: function (router, context) {
						C.Ctl.List.getObjects("Flow");
						router.get('applicationController').connectOutlet({
							viewClass: C.Vw.Flows,
							controller: C.Ctl.List
						});
					},
					enter: function () {
						C.interstitial.show();
					},
					navigateAway: function () {
						C.Ctl.List.unload();
					}
				}),
				streams: Ember.Route.extend({
					route: '/streams',
					connectOutlets: function (router, context) {
						C.Ctl.List.getObjects("Stream");
						router.get('applicationController').connectOutlet({
							viewClass: C.Vw.Flows,
							controller: C.Ctl.List
						});
					},
					enter: function () {
						C.interstitial.show();
					},
					navigateAway: function () {
						C.Ctl.List.unload();
					}
				}),
				datas: Ember.Route.extend({
					route: '/datas',
					connectOutlets: function (router, context) {
						C.Ctl.List.getObjects("DataSet");
						router.get('applicationController').connectOutlet({
							viewClass: C.Vw.Flows,
							controller: C.Ctl.List
						});
					},
					enter: function () {
						C.interstitial.show();
					},
					navigateAway: function () {
						C.Ctl.List.unload();
					}
				})
			})
		})
	});

	$(function () {
		C.initialize();
	});
});