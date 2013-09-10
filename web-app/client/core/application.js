/*
 * Application
 */

define(['core/components', 'core/embeddables/index', 'core/http', 'core/socket',
				'core/util'],
function(Components, Embeddables, HTTP, Socket, Util) {

	var Application = Ember.Application.extend({

		/*
		 * Logs router transitions to the Developer Console
		 */
		LOG_TRANSITIONS: false,

		/*
		 * Constant: Polling interval for metrics.
		 */
		POLLING_INTERVAL: 1000,

		/*
		 * Constant: Delay to wait for Embeddables to configure before checking them.
		 */
		EMBEDDABLE_DELAY: 100,

		/*
		 * Variable: Whether to watch and warn about latency.
		 */
		WATCH_LATENCY: false,

		/*
		 * Allows us to set the ID of the main view element.
		 */
		ApplicationView: Ember.View.extend({
			elementId: 'content'
		}),

		ApplicationController: Ember.Controller.extend({
			/*
			 * A workaround to track the router's current path.
			 */
			updateCurrentPath: function() {
				C.set('currentPath', this.get('currentPath'));
			}.observes('currentPath'),

			load: function () {

				Em.debug('Routing started');

				/*
				 * Do version check.
				 */
				this.HTTP.get('version', this.checkVersion);
			},

			checkVersion: function(version) {

				if (version && version.current !== 'UNKNOWN') {

					if (version.current !== version.newest) {

						$('#warning').html('<div>New version available: ' + version.current + ' » ' +
							version.newest + ' <a target="_blank" href="https://accounts.continuuity.com/">' +
							'Click here to download</a>.</div>').show();
					}
				}
			}
		}),

		setupEnvironment: function (env) {

			C.Env.set('version', env.version);
			C.Env.set('location', env.location);
			C.Env.set('productName', env.product.toLowerCase());

			$('title').text(env.product + ' » Continuuity');

			if (env.account) {

				C.Env.set('authenticated', true);
				C.Env.set('user', env.account);
				C.Env.set('cluster', env.cluster.info);

				$('title').text(C.Env.cluster.vpc_label + ' » Continuuity');

				C.set('WATCH_LATENCY', true);

				Em.debug('Authenticated');

			} else {

				C.Env.set('credential', env.credential || '');
				C.Env.set('user', { id: 'developer' });

			}

			if (env.version && env.version !== 'UNKNOWN') {
				$('#build-number').html(' &#183; BUILD <span>' + env.version + '</span>').attr('title', env.ip);
			}

			/*
			 * Append the product theme CSS
			 */
			var themeLink = this.getThemeLink();
			document.getElementsByTagName("head")[0].appendChild(themeLink);

			/*
			 * Load patch.
			 */
			require([C.Env.productName + '/views/index'], function (Patch) {

				/*
				 * Patching feature not being used yet.
				 */

				/*
				 * Proceed with routing.
				 */
				C.advanceReadiness();
				C.set('initialized', true);

			});

		},

		getThemeLink: function() {
			var themeLink = document.createElement('link');
			themeLink.setAttribute("rel", "stylesheet");
			themeLink.setAttribute("type", "text/css");
			themeLink.setAttribute("href", "/assets/css/" + C.Env.get('productName') + ".css");
			return themeLink;
		},

		setTimeRange: function (millis) {
			this.set('__timeRange', millis);
			this.set('__timeLabel', {
				/*
				86400: 'Last 24 Hours',
				*/
				3600: 'Last 1 Hour',
				600: 'Last 10 Minutes',
				60: 'Last 1 Minute'
			}[millis]);
		},
		__timeRange: 60,
		__timeLabel: 'Last 1 Minute',
		resizeHandlers: {},
		addResizeHandler: function (id, handler) {
			this.resizeHandlers[id] = handler;
		},
		removeResizeHandler: function (id) {
			delete this.resizeHandlers[id];
		},
		/*
		 * Application-level event handlers for Resource events
		 */
		__handlers: {
			'Socket': {
				/*
				 * Called when the socket is (re)connected.
				 */
				'connect': function (env) {

					if (!C.initialized) {
						C.setupEnvironment(env);
					} else {
						$('#warning').html('<div>Reconnected!</div>').fadeOut();
					}
				},
				/*
				 * Called when the socket experiences an error.
				 */
				'error': function (message, args) {

					if (typeof message === "object") {
						message = message.message;
					}
					$('#warning').html('<div>' + message + '</div>').show();

				},
				/*
				 * Called when the socket receives JAR upload status information.
				 */
				'upload': function (status) {
					Util.Upload.update(status);
				}
			}
		},
		/*
		 * Stores an indication of which resources are being mocked.
		 */
		__mocked: {}

	});

	var assignments = {};

	assignments['Embed'] = {};

	/*
	 * Setup Models, Views, Controllers and Embeddables.
	 *
	 * Referenced automatically by Ember. Format:
	 * Model:		Application.Flow
	 * View:		Application.FlowView
	 * Controller:	Application.FlowController
	 */

	Components.forEach(function (component) {

		if (component.kind === 'Embeddable') {
			assignments['Embed'][component.type] = component;
		} else {
			assignments[component.kind === 'Model' ?
			component.type : component.type + component.kind] = component;
		}

	});

	/*
	 * Assign Utility functions.
	 */
	assignments['Util'] = Util;

	/*
	 * Configure upload handler.
	 */
	Util.Upload.configure();

	/*
	 * Shortcut for modal dialogues.
	 */
	assignments['Modal'] = assignments['Embed'].Modal.create();
	assignments['Modal'].append();

	/*
	 * Generic object to store auth / env information.
	 */
	assignments['Env'] = Em.Object.create();

	/*
	 * Updates the Application Object with new properties and values.
	 */
	Application.reopen(assignments);

	/*
	 * Inject default services into our controllers.
	 */
	Em.Application.initializer({
		name: "resources",

		initialize: function(container, application) {

			var resources = [HTTP, Socket];
			var i = resources.length, type, resource;
			while(i--) {

				type = resources[i].type;

				if (!C.__mocked[type]) {
					container.optionsForType(type, { singleton: true });
					container.register(type + ':main', resources[i]);
					/*
					 * This injection actually constructs the resource.
					 */
					container.typeInjection('controller', type, type + ':main');

					/*
					 * Check Application-level event handlers on the resource.
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
			}

			/*
			 * Defer readiness until the Websocket is connected and Patch loaded.
			 * See "advanceReadiness" in the "setupEnvironment" call above.
			 */
			C.deferReadiness();

		}
	});

	/*
	 * Handle window resize events (e.g. for Sparkline resize)
	 */
	var resizeTimeout = null;
	window.onresize = function () {

		clearTimeout(resizeTimeout);
		resizeTimeout = setTimeout(function () {

			for (var h in C.resizeHandlers) {
				if (typeof C.resizeHandlers[h] === 'function') {
					C.resizeHandlers[h]();
				}
			}

		}, 500);

	};

	window.onblur = function () {
		if (C && typeof C.blur === 'function') {
			C.blur();
		}
	};
	window.onfocus = function () {
		if (C && typeof C.focus === 'function') {
			C.focus();
		}
	};

	Em.debug('Application setup complete');

	return Application;

});
