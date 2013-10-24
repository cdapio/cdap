/*
 * Application
 */

define(['core/components', 'core/embeddables/index', 'core/http',
				'core/util'],
function(Components, Embeddables, HTTP, Util) {

	var Application = Ember.Application.extend({

		/*
		 * Logs router transitions to the Developer Console
		 */
		LOG_TRANSITIONS: false,

		/*
		 * Constant: Polling interval for metrics.
		 */
		POLLING_INTERVAL: 5000,

		/*
		 * Constant: Delay to wait for Embeddables to configure before checking them.
		 */
		EMBEDDABLE_DELAY: 100,

		/*
		 * Variable: Whether to watch and warn about latency.
		 */
		WATCH_LATENCY: false,

		/*
		 * Number of points to render on a sparkline.
		 */
		SPARKLINE_POINTS: 60,

		/*
		 * A buffer in seconds to account for metrics system latency.
		 */
		METRICS_BUFFER: 5,

		/*
		 * A buffer in seconds to account for resource metrics system latency.
		 * Higher because resource metrics are only written every 20 seconds.
		 */
		RESOURCE_METRICS_BUFFER: 30,

		/*
		 * Enable or disable local cache.
		 */
		ENABLE_CACHE: typeof Storage !== "undefined",

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
				this.HTTP.get('version', {cache: true}, this.checkVersion);
			},

			checkVersion: function(version) {

				if (version && version.current !== 'UNKNOWN') {

					if (version.current !== version.newest &&
						version.current.indexOf('SNAPSHOT') === -1 &&
						C.get('isLocal')) {

						$('#warning').html('<div>New version available: ' + version.current + ' » ' +
							version.newest + '<br /><a target="_blank" href="https://accounts.continuuity.com/">' +
							'Click here to download</a>.</div>').show();
					}
				}
			}
		}),

		isLocal: function () {

			return this.get('Env.productId') === 'local';

		}.property('Env.productName'),

		isEnterprise: function () {

			return this.get('Env.productId') === 'enterprise';

		}.property('Env.productName'),

		initialize: function (http) {
			var self = this;
			http.get('environment', {cache: true}, function (response) {
				 self.setupEnvironment(response);
			});

		},

		setupEnvironment: function (env) {

			C.Env.set('version', env.product_version);
			C.Env.set('productId', env.product_id);
			C.Env.set('productName', env.product_name);
			C.Env.set('ip', env.ip);

			$('title').text(env.product_name + ' » Continuuity');

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

			/*
			 * Append the product theme CSS
			 */
			var themeLink = this.getThemeLink();
			document.getElementsByTagName("head")[0].appendChild(themeLink);

			/*
			 * Load patch.
			 */
			require([C.Env.productId + '/views/index'], function (Patch) {

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

		ready: function (){

			Em.run.next(function () {

				if (C.Env.version && C.Env.version !== 'UNKNOWN') {
					setTimeout(function () {
						$('#build-number').html('<span>&nbsp;&#183;&nbsp;&nbsp;</span><span>' + C.Env.version + '</span>').attr('title', C.Env.ip);
					}, C.EMBEDDABLE_DELAY);
				}

			});

		},

		getThemeLink: function() {
			var themeLink = document.createElement('link');
			themeLink.setAttribute("rel", "stylesheet");
			themeLink.setAttribute("type", "text/css");
			themeLink.setAttribute("href", "/assets/css/" + C.Env.get('productId') + ".css");
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

			var resources = [HTTP];
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

				}
			}

			/*
			 * Defer readiness until the Websocket is connected and Patch loaded.
			 * See "advanceReadiness" in the "setupEnvironment" call above.
			 */
			C.deferReadiness();
			C.initialize(HTTP.create());

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
