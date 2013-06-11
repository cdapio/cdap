/*
 * Main entry point for Reactor UI
 * Defines routes and attaches mocks
 */

require.config({
	paths: {
		"core": "../core"
	}
});

define (['core/application'], function (Application) {

	/*
	 * Determine whether to swap out specific components with mocks.
	 */
	var mocks = window.location.search.split('?')[1];


	if (mocks) {
		mocks = mocks.split('=')[1];
		if (mocks) {
			mocks = mocks.split(',');
		} else {
			mocks = null;
		}
	} else {
		mocks = null;
	}

	/*
	 * Inject requested mocks into our controllers.
	 */
	if (mocks) {

		Em.Application.initializer({
			name: "mocks",
			before: "resources",

			initialize: function(container, application) {

				var i = mocks.length;
				while (i--) {
					C.__mocked[mocks[i]] = true;
					mocks[i] = 'mocks/' + mocks[i].toLowerCase();
				}

				/*
				 * Note: This is async. The 'resources' initializer is not.
				 */
				require(mocks, function () {

					mocks = Array.prototype.slice.call(arguments, 0);

					var i = mocks.length, type, resource;
					while (i--) {

						type = mocks[i].type;
						container.optionsForType(type, { singleton: true });
						container.register(type + ':main', mocks[i]);
						container.typeInjection('controller', type, type + ':main');

						/*
						 * Check Application-level event handlers on the resource.
						 * E.g. Socket.on('connect');
						 */
						if (typeof C.__handlers[type] === 'object') {

							resource = container.lookup(type + ':main');
							for (var event in C.__handlers[type]) {
								resource.on(event, C.__handlers[type][event]);
							}
							if (typeof resource.connect === 'function') {
								resource.connect();
							}

						}

						/*
						 * Temporary for Backwards Compat.
						 */
						if (type === 'Socket') {
							C.Socket = container.lookup('Socket:main');
							C.get = function () {
								C.Socket.request.apply(C.Socket, arguments);
							};
						}
					}
				});
			}
		});

	}

	/*
	 * Instantiate the Application.
	 */
	window.C = Application.create();

	/*
	 * The following define the routes in use by the application.
	 * Templates are referred to by resource name and inserted automatically.
	 * Models are determined by the dynamic route and loaded automatically.
	 */
	C.Router.map(function() {

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
		/*
		 * Override to load the Controller once the Route has been activated.
		 */
		setupController: function(controller) {
			controller.load();
		},
		/*
		 * Override to unload the Controller once the Route has been deactivated.
		 */
		deactivate: function () {
			this.controller.unload();
		},
		/*
		 * Override to load a model based on parameter name and inject HTTP resource.
		 */
		model: function (params) {
			for (var key in params) {
				/*
				 * Converts e.g. 'app_id' into 'App', 'flow_id' into 'Flow'
				 */
				var type = key.charAt(0).toUpperCase() + key.slice(1, key.length - 3);
				/*
				 * Finds type and injects HTTP
				 */
				if (type in C) {
					return C[type].find(params[key],
						this.controllerFor('Application').HTTP);
				}
			}
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
	 * @param {string} type ['App', 'Stream', 'Flow', ...]
	 */
	function getListHandler(type) {
		return {
			/*
			 * Override to load the Controller once the Route has been activated.
			 */
			setupController: function  () {
				this.controllerFor('List').load(type);
			},
			/*
			 * Override the templates to be rendered and where.
			 */
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
			/*
			 * Override to unload the Controller once the Route has been deactivated.
			 */
			deactivate: function () {
				this.controllerFor('List').unload();
			}
		};
	}

	$.extend(C, {

		StreamsRoute: Em.Route.extend(getListHandler('Stream')),

		FlowsRoute: Em.Route.extend(getListHandler('Flow')),

		DatasetsRoute: Em.Route.extend(getListHandler('Dataset')),

		ProceduresRoute: Em.Route.extend(getListHandler('Procedure'))

	});

});