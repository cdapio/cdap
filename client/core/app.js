//
// Main entrypoint for client-side application.
//

require.config({
	paths: {
		"lib": "../core/lib",
		"models": "../core/models",
		"views": "../core/views",
		"controllers": "../core/controllers",
		"partials": "../core/partials"
	}
});

define(['core/models/index', 'core/views/index', 'core/controllers/index'],
function(Models, Views, Controllers){

	$.timeago = $.timeago || function () {};
    $.timeago.settings.strings.seconds = '%d seconds';
    $.timeago.settings.strings.minute = 'About a minute';
    $.timeago.settings.refreshMillis = 0;

    Date.prototype.ISO8601 = function (date) {
      date = date || this;
      var pad_two = function(n) {
          return (n < 10 ? '0' : '') + n;
      };
      var pad_three = function(n) {
          return (n < 100 ? '0' : '') + (n < 10 ? '0' : '') + n;
      };
      return [
          date.getUTCFullYear(), '-',
          pad_two(date.getUTCMonth() + 1), '-',
          pad_two(date.getUTCDate()), 'T',
          pad_two(date.getUTCHours()), ':',
          pad_two(date.getUTCMinutes()), ':',
          pad_two(date.getUTCSeconds()), '.',
          pad_three(date.getUTCMilliseconds()), 'Z'
      ].join('');
    };

    window.ENV = {};

	var C = window.C = Ember.Application.create({
		debug: function (message) {
			if (!window.ENV.production) {
				console.debug(message);
			}
		},
		ApplicationController: Ember.Controller.extend({
			user: {
				name: "Demo User"
			},
			breadcrumbs: Em.ArrayProxy.create({
				content: function () {
					var path;
					if (C) {
						path = C.router.location.location.hash.split('/');
					} else {
						path = window.location.hash.split('/');
					}

					var crumbs = [];
					var href = ['#'];

					var names = {
						'flows': 'Flows',
						'upload': 'Upload',
						'apps': 'Applications',
						'streams': 'Streams',
						'data': 'Datasets'
					};

					/** Hax. Deals with AppID:FlowID style IDs for flows. **/
					if (path.length && path[path.length - 1].indexOf(':') !== -1) {
						
						var app = path[path.length - 1].split(':')[0];
						var flow = path[path.length - 1].split(':')[1];
						return [
							{
								name: 'Applications',
								href: '#/apps'
							}, {
								name: app,
								href: '#/apps/' + app
							}
						];
					}
					/** End Hax. **/

					for (var i = 1; i < path.length - 1; i ++) {
						href.push(path[i]);
						crumbs.push({
							name: names[path[i]] || path[i],
							href: href.join('/')
						});
					}

					return crumbs;
				}.property('C.router.currentState.name')
			})
		}),
		ApplicationView: Ember.View.extend({
			templateName: 'main',
			elementId: 'content'
		}),
		interstitial: {
			__code: null,
			show: function () {
				$('#interstitial').show();
				return this;
			},
			hide: function (code) {
				if (this.__code) {
					if (this.__code === code) {
						$('#interstitial').fadeOut();
						this.__code = null;
					}
				} else {
					$('#interstitial').fadeOut();
				}
				$('#interstitial').html('<img src="/assets/img/loading.gif" />');
				return this;
			},
			label: function (message) {
				$('#interstitial').html('<h3>' + message + '</h3>').show();
				return this;
			},
			loading: function (message, code) {
				this.__code = code;
				$('#interstitial').html((message ? '<h3>' + message + '</h3>' : '') +
				'<img src="/assets/img/loading.gif" />').show();
				return this;
			}
		},
		ready: function () {

			C.debug('Application ready.');

		},
		util: {
			sparkline: function (widget, data, w, h, m, color) {
		
				var y = d3.scale.linear().domain([d3.max(data), 0]).range([0, h - m]),
					x = d3.scale.linear().domain([0, data.length]).range([0, w]);

				var vis = widget
					.append("svg:svg")
					.attr('width', '100%')
					.attr('height', '100%')
					.attr('preserveAspectRatio', 'none');

				var g = vis.append("svg:g");
				var line = d3.svg.line().interpolate("basis")
					.x(function(d,i) { return x(i); })
					.y(function(d) { return y(d); });

				var p = g.append("svg:path").attr('class', 'sparkline-data').attr("d", line(data));

				return {
					g: g,
					series: {}, // Need to store to track data boundaries
					update: function (name, data) {

						this.series[name] = data;

						var allData = [], length = 0;
						for (var i in this.series) {
							allData = allData.concat(this.series[i]);
							if (this.series[i].length > length) {
								length = this.series[i].length;
							}
						}
						var max = d3.max(allData) || 1;
						var min = d3.min(allData) || -1;
						var extend = Math.round(w * 0.08);

						var yBuffer = 0.0;

						var y = d3.scale.linear().domain([max + (max * yBuffer), min - (min * yBuffer)]).range([m, h - m]),
							x = d3.scale.linear().domain([0, length]).range([extend * -1, w + extend * 2]);

						var line = d3.svg.line().interpolate("basis")
							.x(function(d,i) { return x(i); })
							.y(function(d) { return y(d); });

						this.g.selectAll("path")
							.data([data])
							.attr("transform", "translate(" + x(1) + ")")
							.attr("d", line)
							.transition()
							.ease("linear")
							.duration(1000)
							.attr("transform", "translate(" + x(0) + ")");
					}
				};
			},
			number: function (value) {

				value = Math.abs(value);

				if (value > 1000000000) {
					var digits = 3 - (Math.round(value / 1000000000) + '').length;
					value = value / 1000000000;
					var rounded = Math.round(value * Math.pow(10, digits)) / Math.pow(10, digits);
					value = rounded + 'B';
				} else if (value > 1000000) {
					var digits = 3 - (Math.round(value / 1000000) + '').length;
					value = value / 1000000;
					var rounded = Math.round(value * Math.pow(10, digits)) / Math.pow(10, digits);
					value = rounded + 'M';
				} else if (value > 10000) {
					var digits = 3 - (Math.round(value / 1000) + '').length;
					value = value / 1000;
					var rounded = Math.round(value * Math.pow(10, digits)) / Math.pow(10, digits);
					value = rounded + 'K';
				}

				return value;
			}
		},
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
		Mdl: Models,
		Vw: Views,
		Ctl: Controllers
	});

	function connected (env) {

		window.ENV.isCloud = (env !== 'development');
		C.debug('Environment set to "' + env + '"');

		// This function is called when the socket is (re)connected.

		if (!C.initialized) {

			C.debug('Application connected.');
			
			// Hack: Keep timestamps updated.
			var trigger = 0;
			setInterval( function () {
				trigger++;
				if (C.Ctl.Flow.current) {
					C.Ctl.Flow.current.set('timeTrigger', trigger);
				}
			}, 5000);

			C.initialized = true;
			C.debug('Application initialized.');

		} else {

			// Reconnected.
			C.interstitial.hide();

		}
	}

	function error (message, args) {

		// This function is called when the socket experiences an error.

		if (typeof message === "object") {
			
			if (message.name === "FlowServiceException") {
				$('#flow-alert').removeClass('alert-success')
					.addClass('alert-error').html('Error: ' + message.message).show();

				setTimeout(function () {
					window.location.reload();
				}, 2000);
				return;
			}
			message = message.message;
		}
		C.interstitial.label(message).show();
		
	}

	var socket = io.connect(document.location.hostname);
	var pending = {};
	var current_id = 0;

	C.socket = socket;

	$.extend(socket, {
		request: function (service, request, response, params) {
			if (!service) {
				return;
			}
			request.id = current_id ++;
			pending[request.id] = [response, params];
			this.emit(service, request);
		}
	});

	C.get = function () {
		C.socket.request.apply(C.socket, arguments);
	};

	socket.on('exec', function (error, response) {
		
		if (pending[response.id] &&
			typeof pending[response.id][0] === 'function') {
			pending[response.id][0](error, response, pending[response.id][1]);
			delete pending[response.id];
		}

	});
	socket.on('failure', function (failure) {
		error(failure);
	});
	socket.on('upload', function (response) {
		C.Ctl.Upload.update(response);
	});
	socket.on('connect', function () {

	});
	socket.on('env', connected);
	socket.on('error', function () {
		error('Error', arguments);
	});
	socket.on('connect_failed', function () {
		error('Connection failed.', arguments);
	});
	socket.on('reconnect_failed', function () {
		error('Reconnect failed.', arguments);
	});
	socket.on('reconnect', function () {
		error('Reconnected.', arguments);
	});
	socket.on('reconnecting', function (timeout, attempt) {
		error('Reconnecting. Attempt ' + attempt + '.', arguments);
	});

	C.debug('Models, Views, Controllers loaded and assigned.');

	return C;

});