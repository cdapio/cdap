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

    window.ENV = {
    	account: {
    		account_id: 'developer'
    	}
    };

	var C = window.C = Ember.Application.create({
		debug: function (message) {
			if (!window.ENV.production) {
				console.debug(message);
			}
		},
		ApplicationController: Ember.Controller.extend({
			user: {
				name: ""
			},
			cluster: {
				vpc_label: ""
			},
			breadcrumbs: Em.ArrayProxy.create({
				names: {
					'flows': 'Process',
					'upload': 'Upload',
					'apps': 'Applications',
					'streams': 'Collect',
					'data': 'Store'
				},
				content: function () {

					var path;
					if (C) {
						path = C.router.location.location.hash.split('/');
					} else {
						path = window.location.hash.split('/');
					}

					var crumbs = [];
					var href = ['#'];

					/** Hax. Deals with AppID:FlowID style IDs for flows. **/
					if (path.length && path[path.length - 1].indexOf(':') !== -1) {
						
						var app = path[path.length - 1].split(':')[0];
						var flow = path[path.length - 1].split(':')[1];
						return [
							{
								name: this.names[app] || app,
								href: '#/apps/' + app
							}
						];
					}
					/** Hax. If it's an App, don't link to AppList. **/
					if (path[1] === 'apps') {
						return [];
					}
					/** End Hax. **/

					for (var i = 1; i < path.length - 1; i ++) {
						href.push(path[i]);
						crumbs.push({
							name: this.names[path[i]] || path[i],
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
			//	$('#interstitial').show();
			//	$('#content-body').hide();
				return this;
			},
			hide: function (code) {
				if (this.__code) {
					if (this.__code === code) {
					//	$('#interstitial').fadeOut();
					//	$('#content-body').fadeIn();
						this.__code = null;
					}
				} else {
				//	$('#interstitial').fadeOut();
				//	$('#content-body').fadeIn();
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
				//$('#interstitial').html((message ? '<h3>' + message + '</h3>' : '') +
				//'<img src="/assets/img/loading.gif" />').show();
				return this;
			}
		},
		ready: function () {

			C.debug('Application ready.');

		},
		util: {
			reset: function () {
				C.Vw.Modal.show(
					"Reset AppFabric",
					"You are about to DELETE ALL CONTINUUITY DATA on your cluster. Are you sure you would like to do this?",
					function () {

						C.interstitial.loading('Clearing...');

						C.get('far', {
							method: 'reset',
							params: []
						}, function (error, response) {

							C.interstitial.hide();

							if (error) {

								setTimeout(function () {
									C.Vw.Modal.show(
										"Reset Error",
										error.message
										);
								}, 1000);

							} else {
								C.router.transitionTo('home');
								window.location.reload();
							}

						});
						
					});
				return false;
			},
			sparkline: function (widget, data, w, h, percent) {
				
				var allData = [], length = 0;
				for (var i in this.series) {
					allData = allData.concat(this.series[i]);
					if (this.series[i].length > length) {
						length = this.series[i].length;
					}
				}
				var max = d3.max(allData) || 9;
				var min = d3.min(allData) || -1;
				var extend = Math.round(w / data.length);

				var margin = 5;
				var yBuffer = 0.0;
				var y, x;

				x = d3.scale.linear();//.domain([0, data.length]).range([0, w]);
				y = d3.scale.linear();

				var vis = widget
					.append("svg:svg")
					.attr('width', '100%')
					.attr('height', '100%')
					.attr('preserveAspectRatio', 'none');

				var g = vis.append("svg:g");
				var line = d3.svg.line().interpolate("basis")
					.x(function(d,i) { return x(i); })
					.y(function(d) { return y(d); });

				if (percent) {
					var area = d3.svg.area()
						.x(line.x())
						.y1(line.y())
						.y0(y(0));
					g.append("svg:path").attr('class', 'sparkline-area').attr("d", area(data));
				}

				g.append("svg:path").attr('class', 'sparkline-data').attr("d", line(data));

				return {
					g: g,
					percent: percent,
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
						var max = d3.max(allData) || 100;
						var min = d3.min(allData) || 0;
						var extend = Math.round(w / data.length);

						var yBuffer = 0.0;
						var y, x;

						x = d3.scale.linear().domain([0, length]).range([0 - extend, w - extend]);

						if (this.percent) {
							y = d3.scale.linear()
								.domain([100, 0])
								.range([margin, h - margin]);
						} else {
							if ((max - min) === 0) {
								if (data[0]) {
									max = data[0] + data[0] * 0.1;
									min = data[0] - data[0] * 0.1;
								} else {
									max = 10;
									min = 0;
								}
							}
							y = d3.scale.linear()
								.domain([max + (max * yBuffer), min - (min * yBuffer)])
								.range([margin, h - margin]);
						}

						var line = d3.svg.line().interpolate("basis")
							.x(function(d,i) { return x(i); })
							.y(function(d) { return y(d); });

						if (this.percent) {
							var area = d3.svg.area().interpolate("basis")
								.x(line.x())
								.y1(line.y())
								.y0(y(0));

							this.g.selectAll("path.sparkline-area")
								.data([data])
								.attr("transform", "translate(" + x(1) + ")")
								.attr("d", area)
								.transition()
								.ease("linear")
								.duration(1000)
								.attr("transform", "translate(" + x(0) + ")");
						}

						this.g.selectAll("path.sparkline-data")
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
					digits = digits < 0 ? 2 : digits;
					value = value / 1000000000;
					var rounded = Math.round(value * Math.pow(10, digits)) / Math.pow(10, digits);
					value = rounded + 'B';
				} else if (value > 1000000) {
					var digits = 3 - (Math.round(value / 1000000) + '').length;
					digits = digits < 0 ? 2 : digits;
					value = value / 1000000;
					var rounded = Math.round(value * Math.pow(10, digits)) / Math.pow(10, digits);
					value = rounded + 'M';
				} else if (value > 1000) {
					var digits = 3 - (Math.round(value / 1000) + '').length;
					digits = digits < 0 ? 2 : digits;
					value = value / 1000;
					var rounded = Math.round(value * Math.pow(10, digits)) / Math.pow(10, digits);
					value = rounded + 'K';
				} else {
					var digits = 3 - (value + '').length;
					digits = digits < 0 ? 2 : digits;
					var rounded = Math.round(value * Math.pow(10, digits)) / Math.pow(10, digits);
					value = rounded;
				}

				return value;
			},
			bytes: function (value) {

				if (value > 1073741824) {
					value /= 1073741824;
					return [((Math.round(value * 100) / 100)), 'GB'];
				} else if (value > 1048576) {
					value /= 1048576;
					return [((Math.round(value * 100) / 100)), 'MB'];
				} else if (value > 1024) {
					value /= 1024;
					return [((Math.round(value * 10) / 10)), 'KB'];
				}

				return [value, 'BYTES'];
			}
		},
		onStreams: function () {
			return C.router.currentState.parentState.name === 'streams';
		}.property('C.router.currentState.parentState.name').cacheable(false),
		onDatasets: function () {
			return C.router.currentState.parentState.name === 'datas';
		}.property('C.router.currentState.parentState.name').cacheable(false),
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

	function connected (env) {

		// This function is called when the socket is (re)connected.

		if (!C.initialized) {

			window.ENV.isCloud = (env.location && env.location !== 'development') || false;
			window.ENV.version = env.version;

			if (env.account) {

				window.ENV.account = env.account;
				window.ENV.cluster = env.cluster;
				C.router.applicationController.set('user', env.account);
				C.router.applicationController.set('cluster', env.cluster.info);

			} else {

				window.ENV.credential = env.credential;
				C.router.applicationController.set('user', {
					name: "Developer"
				});
			}

			C.debug('Environment set to "' + env.name + '", version ' + env.version);

			if (env.version && env.version !== 'UNKNOWN') {
				$('#build-number').html(' &#183; BUILD <span>' + env.version + '</span>').attr('title', env.ip);
			}

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
			// C.interstitial.hide();
			$('#warning').html('<div>Reconnected!</div>').fadeOut();

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

		$('#warning').html('<div>' + message + '</div>').show();
//		C.interstitial.label(message).show();
		
	}

	var socket = io.connect(document.location.hostname, {
		secure: document.location.protocol === 'https:'
	});
	var pending = {};
	var current_id = 0;

	C.socket = socket;

	$.extend(socket, {
		request: function (service, request, response, params) {
			if (!service) {
				return;
			}
			request.id = current_id ++;
			pending[request.id] = [response, params, new Date().getTime()];
			this.emit(service, request);
		}
	});

	C.get = function () {
		C.socket.request.apply(C.socket, arguments);
	};

	var warningTimeout;
	var toAverage = [];
	var averageOver = 30;
	var maxResponseTime = 5000;

	socket.on('exec', function (err, response) {
		
		if (err && err.fatal) {

			error(err.fatal);
			delete pending[response.id];
			return;

		}

		if (pending[response.id] &&
			typeof pending[response.id][0] === 'function') {

			if (window.ENV.isCloud) {

				toAverage.push(new Date().getTime() - pending[response.id][2]);

				if (toAverage.length > averageOver) {
					toAverage.shift();
				}

				var i = toAverage.length, sum = 0;
				while (i--) {
					sum += toAverage[i];
				}

				if(sum / toAverage.length > maxResponseTime) {

					clearTimeout(warningTimeout);
					$('#warning').fadeIn();
					warningTimeout = null;

				} else {

					if (warningTimeout === null) {
						warningTimeout = setTimeout(function () {
							$('#warning').fadeOut();
							warningTimeout = null;
						}, 1000);
					}
				}
			}

			pending[response.id][0](err, response, pending[response.id][1]);
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
		error('Disconnected. Attempting to reconnect. (' + attempt + ')', arguments);
	});

	C.debug('Configuration done.');

	return C;

});