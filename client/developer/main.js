
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
						C.Ctl.Dashboard.load();
						router.get('applicationController').connectOutlet({
							viewClass: C.Vw.Dash,
							controller: C.Ctl.Dashboard
						});
					},
					enter: function () {
						C.interstitial.show();
					},
					navigateAway: function () {
						C.Ctl.Dashboard.unload();
					},
				}),
				apps: Ember.Route.extend({
					route: '/apps',
					index: Ember.Route.extend({
						route: '/',
						connectOutlets: function (router, context) {
							C.Ctl.List.getObjects("App");
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.ListPage,
								controller: C.Ctl.List
							});
						},
						navigateAway: function () {
							C.Ctl.List.unload();
						}
					}),
					enter: function () {
						C.interstitial.show();
					},
					app: Ember.Route.extend({
						route: '/:appId',
						connectOutlets: function (router, context) {
							C.Ctl.App.load(context.appId);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.App,
								controller: C.Ctl.App
							});
						},
						navigateAway: function () {
							C.Ctl.App.unload();
						}
					})
				}),
				flows: Ember.Route.extend({
					route: '/flows',
					index: Ember.Route.extend({
						route: '/',
						connectOutlets: function (router, context) {
							C.Ctl.List.getObjects("Flow");

							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.ListPage,
								controller: C.Ctl.List
							});
						},
						navigateAway: function () {
							C.Ctl.List.unload();
						}
					}),
					flow: Ember.Route.extend({
						route: '/:id',
						connectOutlets: function (router, context) {
							var id = context.id.split(':');
							C.Ctl.Flow.load(id[0], id[1]);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.Flow,
								controller: C.Ctl.Flow
							});
						},
						navigateAway: function () {
							C.Ctl.Flow.unload();
						}
					}),
					enter: function () {
						C.interstitial.show();
					}
				}),
				streams: Ember.Route.extend({
					route: '/streams',
					index: Ember.Route.extend({
						route: '/',
						connectOutlets: function (router, context) {
							C.Ctl.List.getObjects("Stream");
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.ListPage,
								controller: C.Ctl.List
							});
						},
						navigateAway: function () {
							C.Ctl.List.unload();
						}
					}),
					stream: Ember.Route.extend({
						route: '/:id',
						connectOutlets: function (router, context) {
							C.Ctl.Stream.load(context.id);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.Stream,
								controller: C.Ctl.Stream
							});
						},
						navigateAway: function () {
							C.Ctl.Stream.unload();
						}
					}),
					enter: function () {
						C.interstitial.show();
					}
				}),
				datas: Ember.Route.extend({
					route: '/data',
					index: Ember.Route.extend({
						route: '/',
						connectOutlets: function (router, context) {
							C.Ctl.List.getObjects("DataSet");
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.ListPage,
								controller: C.Ctl.List
							});
						},
						navigateAway: function () {
							C.Ctl.List.unload();
						}
					}),
					data: Ember.Route.extend({
						route: '/:id',
						connectOutlets: function (router, context) {
							C.Ctl.DataSet.load(context.id);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.DataSet,
								controller: C.Ctl.DataSet
							});
						},
						navigateAway: function () {
							C.Ctl.DataSet.unload();
						}
					}),
					enter: function () {
						C.interstitial.show();
					},

				})
			})
		})
	});

	$(function () {
		C.initialize();
	});
});