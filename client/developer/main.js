
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
		Router: Em.Router.extend({
			enableLogging: false,
			root: Em.Route.extend({
				home: Em.Route.extend({
					route: '/',
					connectOutlets: function (router, context) {
						C.Ctl.Dashboard.load();
						router.get('applicationController').connectOutlet({
							viewClass: C.Vw.Dash,
							controller: C.Ctl.Dashboard
						});
					},
					enter: C.interstitial.show,
					navigateAway: function () {
						C.Ctl.Dashboard.unload();
					}
				}),
				apps: Em.Route.extend({
					route: '/apps',
					index: Em.Route.extend({
						route: '/',
						connectOutlets: function (router, context) {
							C.Ctl.List.getObjects("Application");
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.ListPage,
								controller: C.Ctl.List
							});
						},
						navigateAway: function () {
							C.Ctl.List.unload();
						},
						enter: C.interstitial.show
					}),
					app: Em.Route.extend({
						route: '/:appId',
						connectOutlets: function (router, context) {
							C.Ctl.Application.load(context.appId);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.Application,
								controller: C.Ctl.Application
							});
						},
						navigateAway: function () {
							C.Ctl.Application.unload();
						},
						enter: C.interstitial.show
					})
				}),
				flows: Em.Route.extend({
					route: '/flows',
					index: Em.Route.extend({
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
						},
						enter: C.interstitial.show
					}),
					flow: Em.Route.extend({
						route: '/status/:id',
						connectOutlets: function (router, context) {

							C.Ctl.FlowHistory.unload();

							var id = context.id.split(':');
							C.Ctl.Flow.load(id[0], id[1]);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.FlowStatus,
								controller: C.Ctl.Flow
							});
						},
						navigateAway: function () {
							C.Ctl.Flow.unload();
						},
						enter: C.interstitial.show
					}),
					history: Em.Route.extend({
						route: '/history/:id',
						connectOutlets: function (router, context) {

							C.Ctl.Flow.unload();

							var id = context.id.split(':');
							C.Ctl.FlowHistory.load(id[0], id[1]);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.FlowHistory,
								controller: C.Ctl.FlowHistory
							});
						},
						navigateAway: function () {
							C.Ctl.FlowHistory.unload();
						},
						enter: C.interstitial.show
					})
				}),
				streams: Em.Route.extend({
					route: '/streams',
					index: Em.Route.extend({
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
						},
						enter: C.interstitial.show
					}),
					stream: Em.Route.extend({
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
						},
						enter: C.interstitial.show
					})
				}),
				queries: Em.Route.extend({
					route: '/queries',
					index: Em.Route.extend({
						route: '/',
						connectOutlets: function (router, context) {
							C.Ctl.List.getObjects("Query");
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.ListPage,
								controller: C.Ctl.List
							});
						},
						navigateAway: function () {
							C.Ctl.List.unload();
						},
						enter: C.interstitial.show
					}),
					query: Em.Route.extend({
						route: '/:id',
						connectOutlets: function (router, context) {
							C.Ctl.Query.load(context.id);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.Query,
								controller: C.Ctl.Query
							});
						},
						navigateAway: function () {
							C.Ctl.Query.unload();
						},
						enter: C.interstitial.show
					})
				}),
				datas: Em.Route.extend({
					route: '/data',
					index: Em.Route.extend({
						route: '/',
						connectOutlets: function (router, context) {
							C.Ctl.List.getObjects("Dataset");
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.ListPage,
								controller: C.Ctl.List
							});
						},
						navigateAway: function () {
							C.Ctl.List.unload();
						},
						enter: C.interstitial.show
					}),
					data: Em.Route.extend({
						route: '/:id',
						connectOutlets: function (router, context) {
							C.Ctl.Dataset.load(context.id);
							router.get('applicationController').connectOutlet({
								viewClass: C.Vw.Dataset,
								controller: C.Ctl.Dataset
							});
						},
						navigateAway: function () {
							C.Ctl.Dataset.unload();
						},
						enter: C.interstitial.show
					})
				})
			})
		})
	});

	$(function () {
		C.initialize();
	});
});