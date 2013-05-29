/*
 * Application
 */

require.config({
	paths: {
		"lib": "../core/lib",
		"models": "../core/models",
		"views": "../core/views",
		"controllers": "../core/controllers",
		"embeddables": "../core/embeddables",
		"partials": "../core/partials"
	}
});

define(['core/components', 'core/api', 'core/embeddables/index', 'core/util'],
function(Components, Api, Embeddables, Util){

	var Application = Ember.Application.extend({
		/*
		 * Logs router transitions to the Developer Console
		 */
		LOG_TRANSITIONS: true,

		/*
		 * Constant: Polling interval for metrics.
		 */
		POLLING_INTERVAL: 1000,

		/*
		 * Constant: Delay to wait for Embeddables to configure before checking them.
		 */
		EMBEDDABLE_DELAY: 100,

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

			}

		}),

		setTimeRange: function (millis) {
			this.set('__timeRange', millis);
			this.set('__timeLabel', {
				86400: 'Last 24 Hours',
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
		}

	});

	var assignments = {};

	/*
	 * Setup Models, Views and Controllers.
	 *
	 * Referenced automatically by Ember. Format:
	 * Model:		Application.Flow
	 * View:		Application.FlowView
	 * Controller:	Application.FlowController
	 *
	 */

	Components.forEach(function (component) {

		assignments[component.kind === 'Model' ?
			component.type : component.type + component.kind] = component;

	});

	/*
	 * Assign API and socket connection handlers.
	 */
	assignments['Api'] = Api;

	/*
	 * Called when the socket is (re)connected.
	 */
	Api.Socket.addConnectedHandler(function (env) {

		if (!C.initialized) {

			C.set('version', env.version);
			C.set('location', env.location);

			if (env.account) {

				C.Env.set('authenticated', true);
				C.Env.set('user', env.account);
				C.Env.set('cluster', env.cluster.info);

				C.Api.Socket.set('watchLatency', true);

				Em.debug('Authenticated');

				$('title').text('Sandbox » Continuuity');

			} else {

				C.Env.set('credential', env.credential);
				C.Env.set('user', { id: 'developer' });

				$('title').text('Developer » Continuuity');

			}

			if (env.version && env.version !== 'UNKNOWN') {
				$('#build-number').html(' &#183; BUILD <span>' + env.version + '</span>').attr('title', env.ip);
			}

			C.advanceReadiness();
			C.set('initialized', true);

			/*
			 * Do version check.
			 */
			$(function () {

				$.getJSON('/version', function (version) {

					if (version && version.current !== 'UNKNOWN') {

						if (version.current !== version.newest) {

							$('#warning').html('<div>New version available: ' + version.current + ' » ' +
								version.newest + ' <a target="_blank" href="https://accounts.continuuity.com/">' +
								'Click here to download</a>.</div>').show();

						}

					}

				});

			});

		} else {

			$('#warning').html('<div>Reconnected!</div>').fadeOut();

		}
	});

	/*
	 * Called when the socket experiences an error.
	 */
	Api.Socket.addErrorHandler(function error (message, args) {

		if (typeof message === "object") {
			message = message.message;
		}
		$('#warning').html('<div>' + message + '</div>').show();

	});

	/*
	 * Assign Embeddables for reference within Handlebars view helpers.
	 */
	assignments['Embed'] = {};

	Embeddables.forEach(function (embeddable) {

		assignments['Embed'][embeddable.type] = embeddable;

	});

	/*
	 * Assign Utility functions.
	 */
	assignments['Util'] = Util;

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
	 * Instantiate the Application.
	 */
	window.C = Application.create();

	/*
	 * Temporary shortcut to invoke a method on the API socket. Deprecating since it collides.
	 */
	C.get = function () {
		C.Api.Socket.request.apply(C.socket, arguments);
	};

	/*
	 * Defer readiness until we have connected the Websocket.
	 * See "advanceReadiness" in the connection handler above.
	 */
	C.deferReadiness();

	/*
	 * Connect the Websocket.
	 */
	C.Api.connect();

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

	Em.debug('Setup complete');

	return C;

});