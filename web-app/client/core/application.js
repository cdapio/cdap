/*
 * Application
 */

define(['core/components', 'core/embeddables/index', 'core/http',
				'core/util'],
function(Components, Embeddables, HTTP, Util) {

  /**
   * Interval where services are called to check.
   */
  var SERVICES_INTERVAL = 3000;

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
		ENABLE_CACHE: false,

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

				if (version && version.current !== 'UNKNOWN' &&
					version.current.indexOf('SNAPSHOT') === -1) {

					var display = false;
					var current = version.current.split(/\./);
					var newest = version.newest.split(/\./);

					current = {
						major: +current[0],
						minor: +current[1],
						revision: +current[2]
					};

					newest = {
						major: +newest[0],
						minor: +newest[1],
						revision: +newest[2]
					};

					if (newest.major === current.major) {
						if (newest.minor === current.minor) {
							if (newest.revision > current.revision) {
								display = true;
							}
						} else {
							if (newest.minor > current.minor) {
								display = true;
							}
						}

					} else {
						if (newest.major > current.major) {
							display = true;
						}
					}

					if (display && C.get('isLocal')) {
						$('#warning .warning-text').html('New version available: ' + newest.major +
							'.' + newest.minor + '.' + newest.revision +
							'<br /><a target="_blank" href="https://www.continuuity.com/download">' +
							'Click here to download</a>.');
						$('#warning').show();

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
			http.get('environment', function (response) {
				 self.setupEnvironment(response);
			});

		},

		/**
		 * Sets up authentication on the global Ember application.
		 */
		setupAuth: function (routeHandler, callback) {
			/**
			 * Recieves response of type {token: <token>}
			 */
			HTTP.create().get('getsession', function (resp) {
				if ('token' in resp) {
					C.Env.set('auth_token', resp.token);
				} else {
					C.Env.set('auth_token', '');
				}
				if (routeHandler !== undefined && !C.Env.get('auth_token') && 'routeName' in routeHandler) {
					routeHandler.transitionTo('Login');
				} else if (typeof callback === 'function') {
				  callback();
				}
			});
		},

//    /**
//     * Determines readiness of Reactor by polling all services and checking status.
//     * @param routeHandler Object Ember route handler.
//     * @param callback Function to execute.
//     */
//    checkReactorReadiness: function (routeHandler, callback) {
//      HTTP.create().rest('system/services/status', function (statuses, callStatus) {
//      	if (callStatus !== 200) {
//      		routeHandler.transitionTo('Loading');
//      		return;
//      	}
//        if (routeHandler !== undefined && 'routeName' in routeHandler) {
//          if (C.Util.isLoadingComplete(statuses)) {
//            routeHandler.transitionTo(routeHandler.routeName);
//          } else {
//            routeHandler.transitionTo('Loading');
//          }
//        }
//        if (callback && typeof callback === 'function') {
//          callback();
//        }
//      });
//    },

		setupEnvironment: function (env) {

			C.Env.set('version', env.product_version);
			C.Env.set('productId', env.product_id);
			C.Env.set('productName', env.product_name);
			C.Env.set('ip', env.ip);
			C.Env.set('nux', !!env.nux);
      C.Env.set('security_enabled', env.security_enabled);

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

				if (C.get('isLocal') && C.Env.get('nux')) {
					C.Util.NUX.start();
				}

				// Scans support links to popup the feedback widget.
				if (window.UserVoice !== undefined) {
					UserVoice.scan();
				}

			});

		},

		getThemeLink: function() {
			var themeLink = document.createElement('link');
			themeLink.setAttribute("rel", "stylesheet");
			themeLink.setAttribute("type", "text/css");
			themeLink.setAttribute("href", "/assets/css/new/" + C.Env.get('productId') + ".css");
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
		routeHandlers: {},
		addRouteHandler: function (id, handler) {
			this.routeHandlers[id] = handler;
		},
		removeRouteHandler: function (id) {
			delete this.routeHandlers[id];
		},
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
			var http = HTTP.create();
			C.initialize(http);
			if (C.Env.security_enabled) {
			  C.setupAuth();
			}

      /**
       * Call services on a per second interval and update icon if any fail.
       */
      //Call initially first so there is no wait, then call every 3 seconds.
      Em.run(function() {
        callServiceStatus()
        setInterval(function () {
          callServiceStatus();
        }, SERVICES_INTERVAL);
      });

		}
	});

  /**
   * Calls the service status endpoint and sets the notification status that displays number of failed services.
   */
  var callServiceStatus = function () {
    HTTP.create().rest('system/services/status', function (statuses, callStatus) {
      var failedStatusCount = 0;
      if (Object.prototype.toString.call(statuses) == '[object Object]') {
	      for (var status in statuses) {
	        if (statuses.hasOwnProperty(status) && statuses[status] !== 'OK') {
	          failedStatusCount++;
	        }
	      }
	      if (failedStatusCount) {
	        $("#failed-services").html(failedStatusCount);
	        $("#failed-services-container").show();
	      } else {
	        $("#failed-services-container").hide();
	      }      	
      }
    });
};

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
		if (C !== undefined && typeof C.blur === 'function') {
			C.blur();
		}
	};
	window.onfocus = function () {
		if (C !== undefined && typeof C.focus === 'function') {
			C.focus();
		}
	};

	Em.run.next(function() {			
		$('#warning-close').click(function() {
			$('#warning').hide();
		});
	});

	Em.debug('Application setup complete');

	return Application;

});
