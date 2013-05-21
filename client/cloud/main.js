
/*
 * Main entry point for Hosted Reactor UI
 */

require.config({
	paths: {
		"core": "../core/"
	}
});

/*
 * Patching feature not being used yet.
 */

define (['core/application', 'patch/views/index'], function (C, Patch) {

	C.Router.map(function() {

		/*
		 * The following define the routes in use by the application.
		 * Templates are referred to by resource name and inserted automatically.
		 * Models are determined by the dynamic route and loaded automatically.
		 */

		this.resource('App', { path: '/apps/:app_id' } );

		this.resource('Streams', { path: '/streams' });
		this.resource('Stream', { path: '/streams/:stream_id' });

		this.resource('Flows', { path: '/flows' });
		this.resource('Flow', { path: '/flows/:flow_id' }, function() {

			this.resource('FlowStatus', { path: '/' }, function () {

				// These live in FlowStatus so they can visually overlay the Flow.
				this.route('Flowlet', { path: '/flowlets/:flowlet_id' });
				this.route('Stream', { path: '/streams/:stream_id' });

			});

			this.route('Log', { path: '/log' });
			this.route('History', { path: '/history' });

		});

		this.resource('Datasets', { path: '/datasets' });
		this.resource('Dataset', { path: '/datasets/:dataset_id' });

		this.resource('Procedures', { path: '/procedures' });
		this.resource('Procedure', { path: '/procedures/:procedure_id' }, function () {

			this.route('Status', { path: '/' });
			this.route('Log', { path: '/log' });

		});

	});

	/*
	 * This is a basic route handler that others can extend from to reduce duplication.
	 */
	var basicRouter = Ember.Route.extend({
		setupController: function(controller) {
			controller.load();
		},
		deactivate: function () {
			this.controller.unload();
		}
	});

	/*
	 * The following define the actual route handlers.
	 * Dashboard controller is the "Index" route handler, as specified in its source.
	 */
	$.extend(C, {

		ApplicationRoute: basicRouter.extend(),

		IndexRoute: basicRouter.extend(),

		AppRoute: basicRouter.extend(),

		StreamRoute: basicRouter.extend(),

		FlowStatusRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Flow');
			}
		}),

		FlowLogRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Flow');
			}
		}),

		FlowHistoryRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Flow');
			}
		}),

		FlowStatusFlowletRoute: basicRouter.extend({
			model: function (params) {
				// See FlowletController to see how we get the full Flowlet model.
				return C.Flowlet.create({ 'name': params.flowlet_id });
			}
		}),

		FlowStatusStreamRoute: basicRouter.extend(),

		DatasetRoute: basicRouter.extend(),

		ProcedureStatusRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Procedure');
			}
		}),

		ProcedureLogRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Procedure');
			}
		})

	});

	/*
	 * Pages for lists of Elements use the List controller.
	 */
	function getListHandler(type) {
		return {
			setupController: function  () {
				this.controllerFor('List').load(type);
			},
			renderTemplate: function () {
				/*
				 * Render the List Page template (i.e. the header / time selector)
				 */
				this.render('list-page', {
					controller: 'List'
				});
				/*
				 * Render a list type partial into the List Page template
				 */
				this.render('_' + type.toLowerCase() + 's-list', {
					controller: 'List',
					into: 'list-page'
				});
			},
			deactivate: function () {
				this.controllerFor('List').unload();
			}
		}
	}

	$.extend(C, {

		StreamsRoute: Em.Route.extend(getListHandler('Stream')),

		FlowsRoute: Em.Route.extend(getListHandler('Flow')),

		DatasetsRoute: Em.Route.extend(getListHandler('Dataset')),

		ProceduresRoute: Em.Route.extend(getListHandler('Procedure'))

	});

});