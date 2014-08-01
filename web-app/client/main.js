/*
 * Main entry point for Reactor UI
 */

define (['core/application', 'helpers/localstorage-adapter'], function (Application, SSAdapter) {

	/*
	 * Instantiate the Application.
	 */
	window.C = Application.create();

	if (C.ENABLE_CACHE) {
		/**
		 * Add storage adapter.
		 */
		C.SSAdapter = new SSAdapter('continuuity', '/rest/apps', 5000);

		C.SSAdapter.on('cacheExpired', function () {
			$('#warning').html('<div>This page has updated since you last opened it. Please' +
			' <span id="warning-reload">reload</a>.</div>').show();
			$('#warning-reload').click(function () {
				window.location.reload();
			});
		});
	}

	/*
	 * Temporary hold for Tree controls. (Resource View)
	 */
	C.TreeBranchController = Ember.ObjectController.extend({});
	C.register('controller:treeBranch', C.TreeBranchController, { singleton: false });

	C.TreeBranchView = Ember.View.extend({
		tagName: 'ul',
		templateName: 'tree-branch',
		classNames: ['tree-branch']
	});

	C.TreeNodeController = Ember.ObjectController.extend({
		isExpanded: false,
		toggle: function() {
			this.set('isExpanded', !this.get('isExpanded'));
		},
		click: function() {
			console.log('Clicked: ' + this.get('text'));
		}
	});
	C.register('controller:treeNode', C.TreeNodeController, { singleton: false });

	C.TreeNodeView = Ember.View.extend({
		tagName: 'li',
		templateName: 'tree-node',
		classNames: ['tree-node']
	});

	/*
	 * The following define the routes in use by the application.
	 * Templates are referred to by resource name and inserted automatically.
	 * Models are determined by the dynamic route and loaded automatically.
	 */
	C.Router.map(function() {

		this.resource('Loading', { path: '/loading' } );
    this.resource('ConnectionError', { path: '/connectionerror' } );
		this.resource('Services', { path: '/services' } );
    this.resource('Service', { path: '/services/:service_id' }, function() {
      this.route('Log', { path: '/log' });
    });

		this.resource('Login', { path: '/login' } );
		this.resource('AccessToken', { path: '/accesstoken' } );

		this.resource('Overview', { path: '/overview' } );

		this.resource('Resources', { path: '/resources' } );

		this.resource('App', { path: '/apps/:app_id' } );

		this.resource('Streams', { path: '/streams' });
		this.resource('Stream', { path: '/streams/:stream_id' });

		this.resource('Flows', { path: '/flows' });
		this.resource('Flow', { path: '/flows/:flow_id' }, function() {

			this.resource('FlowStatus', { path: '/' }, function () {

				// These live in FlowStatus so they can visually overlay the Flow.
				this.route('Flowlet', { path: '/flowlets/:flowlet_id' });
				this.route('Stream', { path: '/streams/:stream_id' });
				this.route('Config', { path: '/config' });

			});

			this.route('Log', { path: '/log' });
			this.route('History', { path: '/history' });

		});

		this.resource('Datasets', { path: '/datasets' });
		this.resource('Dataset', { path: '/datasets/:dataset_id' });

		this.resource('Procedures', { path: '/procedures' });
		this.resource('Procedure', { path: '/procedures/:procedure_id' }, function () {

			this.resource('ProcedureStatus', { path: '/' }, function () {
				// These live in ProcedureStatus so they can visually overlay the Procedure.
				this.route('Config', { path: '/config' });
			});

			this.route('Log', { path: '/log' });

		});

		this.resource('Mapreduce', {path: '/mapreduce/:mapreduce_id'}, function() {

			this.resource('MapreduceStatus', { path: '/' }, function () {
				// These live in MapreduceStatus so they can visually overlay the Mapreduce Job.
				this.route('Config', { path: '/config' });
			});

			this.route('Log', { path: '/log'});
			this.route('History', { path: '/history'});

		});

		this.resource('Workflow', {path: '/workflows/:workflow_id'}, function () {
			this.resource('WorkflowStatus', {path: '/'}, function () {
				this.route('Config', { path: '/config'});
			});
			this.route('History', { path: '/history' });
		});

		this.route('Analyze', { path: '/analyze' });

		this.route("PageNotFound", { path: "*:" });

	});

	function modelFinder (params) {

		for (var key in params) {
      if (params.hasOwnProperty(key)) {
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
	}

	/*
	 * This is a basic route handler that others can extend from to reduce duplication.
	 */
	var basicRouter = Ember.Route.extend({
		/**
		 * Check auth on every route transition.
		 */
		activate: function() {
		  var routeHandler = this;
		  if (C.Env.security_enabled) {
		    C.setupAuth(routeHandler)
		  }
		},

		/*
		 * Override to load the Controller once the Route has been activated.
		 */
		setupController: function(controller, model) {
			controller.set('model', model);
			controller.load();

			var handlers = C.get('routeHandlers');
			for (var handler in handlers) {
				if (typeof handlers[handler] === 'function') {
					handlers[handler](controller, model);
				}
			}

			window.scrollTo(0, 0);
		},
		/*
		 * Override to unload the Controller once the Route has been deactivated.
		 */
		deactivate: function () {
      if ('controller' in this) {
        this.controller.unload();
      }
		},
		/*
		 * Override to load a model based on parameter name and inject HTTP resource.
		 */
		model: modelFinder

	});

  /*
   * Pages for lists of Elements use the List controller.
   * @param {string} type ['App', 'Stream', 'Flow', ...]
   */
  function getListHandler(types) {
    return {
      /**
       * Check auth on every route transition.
       */
      activate: function() {
        var routeHandler = this;
        if (C.Env.security_enabled) {
          C.setupAuth(routeHandler)
        }
      },
      /*
       * Override to load the Controller once the Route has been activated.
       */
      setupController: function  () {
        for (var i=0, len=types.length; i<len; i++) {
          this.controllerFor('List').load(types[i]);
        }
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
        for (var i=0, len=types.length; i<len; i++) {
          this.render('_' + types[i].toLowerCase() + 's-list', {
            controller: 'List',
            into: 'list-page'
          });
        }
      },
      /*
       * Override to unload the Controller once the Route has been deactivated.
       */
      deactivate: function () {
        this.controllerFor('List').unload();
      }
    };
  }

	/*
	 * The following define the actual route handlers.
	 */
	$.extend(C, {

		ApplicationRoute: basicRouter.extend(),

		IndexRoute: Ember.Route.extend({
      model: modelFinder,
      redirect: function() {
        this.transitionTo('Overview');
      }
    }),

    ServicesRoute: basicRouter.extend(),

    ServiceRoute: Ember.Route.extend({
      model: modelFinder
    }),

    ServiceLogRoute: basicRouter.extend({
      model: function () {
        return this.modelFor('Service');
      },
      renderTemplate: function () {
        this.render('Runnable/Log');
      }
    }),

    LoginRoute: basicRouter.extend(),

    AccessTokenRoute: basicRouter.extend(),

		OverviewRoute: basicRouter.extend(),

		ResourcesRoute: basicRouter.extend(),

		AppRoute: basicRouter.extend(),

		StreamRoute: basicRouter.extend(),

		/*
		 * Ensures that the HTTP injection is handled properly (see basicRouter)
		 */
		FlowRoute: Ember.Route.extend({
			model: modelFinder
		}),

		FlowStatusRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Flow');
			}
		}),

		/*
		 * This will use the FlowLogController with the RunnableLog template.
		 * FlowLogController extends RunnableLogController.
		 */
		FlowLogRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Flow');
			},
			renderTemplate: function () {
				this.render('Runnable/Log');
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

		/*
		 * This will use the FlowStatusConfigController with the RunnableConfig template.
		 * FlowStatusConfigController extends RunnableConfigController.
		 */
		FlowStatusConfigRoute: basicRouter.extend({
			renderTemplate: function () {
				this.render('Runnable/Config');
			}
		}),

		/*
		 * Ensures that the model is handled properly (see basicRouter)
		 */
		MapreduceRoute: Ember.Route.extend({
			model: modelFinder
		}),

		MapreduceStatusRoute: basicRouter.extend({
			model: function() {
				return this.modelFor('Mapreduce');
			}
		}),

		MapreduceLogRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Mapreduce');
			},
			renderTemplate: function () {
				this.render('Runnable/Log');
			}
		}),

		MapreduceHistoryRoute: basicRouter.extend({
			model: function() {
				return this.modelFor('Mapreduce');
			}
		}),

		MapreduceStatusConfigRoute: basicRouter.extend({
			renderTemplate: function () {
				this.render('Runnable/Config');
			}
		}),

		/*
		 * Ensures that the model is handled properly (see basicRouter)
		 */
		WorkflowRoute: Ember.Route.extend({
			model: modelFinder
		}),

		WorkflowStatusRoute: basicRouter.extend({
			model: function() {
				return this.modelFor('Workflow');
			}
		}),

		WorkflowHistoryRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Workflow');
			}
		}),

		WorkflowStatusConfigRoute: basicRouter.extend({
			renderTemplate: function () {
				this.render('Runnable/Config');
			}
		}),

		DatasetRoute: basicRouter.extend(),

		/*
		 * Ensures that the HTTP injection is handled properly (see basicRouter)
		 */
		ProcedureRoute: Ember.Route.extend({
			model: modelFinder
		}),

		ProcedureStatusRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Procedure');
			}
		}),

		ProcedureLogRoute: basicRouter.extend({
			model: function () {
				return this.modelFor('Procedure');
			},
			renderTemplate: function () {
				this.render('Runnable/Log');
			}
		}),

		/*
		 * This will use the FlowStatusConfigController with the RunnableConfig template.
		 * FlowStatusConfigController extends RunnableConfigController.
		 */
		ProcedureStatusConfigRoute: basicRouter.extend({
			renderTemplate: function () {
				this.render('Runnable/Config');
			}
		}),

		AnalyzeRoute: basicRouter.extend(),

		PageNotFoundRoute: Ember.Route.extend(),

    LoadingRoute: basicRouter.extend(),

    ConnectionErrorRoute: basicRouter.extend()

	});

	$.extend(C, {

		StreamsRoute: Em.Route.extend(getListHandler(['Stream'])),

		FlowsRoute: Em.Route.extend(getListHandler(['Flow', 'Mapreduce', 'Workflow'])),

		WorkflowsRoute: Em.Route.extend(getListHandler(['Workflow'])),

		DatasetsRoute: Em.Route.extend(getListHandler(['Dataset'])),

		ProceduresRoute: Em.Route.extend(getListHandler(['Procedure']))

	});

	return C;
});

/**
 * Helper to make equality work in Handlebars templates.
 */
Handlebars.registerHelper('ifCond', function (v1, operator, v2, options) {
  switch (operator) {
    case '==':
      return (this.get(v1) == v2) ? options.fn(this) : options.inverse(this);
    case '===':
      return (this.get(v1) === v2) ? options.fn(this) : options.inverse(this);
    case '<':
      return (this.get(v1) < v2) ? options.fn(this) : options.inverse(this);
    case '<=':
      return (this.get(v1) <= v2) ? options.fn(this) : options.inverse(this);
    case '>':
      return (this.get(v1) > v2) ? options.fn(this) : options.inverse(this);
    case '>=':
      return (this.get(v1) >= v2) ? options.fn(this) : options.inverse(this);
    case '&&':
      return (this.get(v1) && v2) ? options.fn(this) : options.inverse(this);
    case '||':
      return (this.get(v1) || v2) ? options.fn(this) : options.inverse(this);
    default:
      return options.inverse(this);
  }
});