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

	console.log('Application loaded.');

	var App = window.App = Ember.Application.create({
		ApplicationController: Ember.Controller.extend({
			user: {
				name: "Don"
			},
			breadcrumbs: Em.ArrayProxy.create({
				content: function () {
					var path;
					if (App) {
						path = App.router.location.location.hash.split('/');
					} else {
						path = window.location.hash.split('/');
					}

					console.log(path);

					var crumbs = [];
					var href = ['#'];

					var names = {
						'flows': 'Dashboard',
						'upload': 'Upload'
					};

					for (var i = 1; i < path.length; i ++) {
						href.push(path[i]);
						crumbs.push({
							name: names[path[i]] || path[i],
							href: href.join('/')
						});
					}
					console.log('RETURNING', crumbs);
					return crumbs;
				}.property('App.router.currentState')
			})
		}),
		ApplicationView: Ember.View.extend({
			templateName: 'application',
			elementId: 'content'
		}),
		interstitial: {
			show: function () {
				$('#interstitial').fadeIn();//show();
				return this;
			},
			hide: function () {
				$('#interstitial').fadeOut();//hide();
				return this;
			},
			label: function (message) {
				$('#interstitial').html('<h3>' + message + '</h3>').show();
				return this;
			},
			loading: function (message) {
				$('#interstitial').html((message ? '<h3>' + message + '</h3>' : '') +
				'<img src="/assets/img/loading.gif" />').show();
				return this;
			}
		},
		ready: function () {

			console.log('Application ready.');

		},
		util: {
			sparkline: function (widget, data, w, h, m, color) {
		
				var y = d3.scale.linear().domain([d3.max(data), 0]).range([0 + m, h + m]),
					x = d3.scale.linear().domain([0, data.length]).range([0 + m, w + m]);

				var vis = widget
					.append("svg:svg")
					.attr("width", w)
					.attr("height", h);
				var g = vis.append("svg:g");
				var line = d3.svg.line().interpolate("basis")
					.x(function(d,i) { return x(i); })
					.y(function(d) { return y(d); });

				var p = g.append("svg:path");
				p.attr('class', 'sparkline-data').attr("d", line(data));
				p.append("svg:circle")
					.attr("r", 5)
					.style("fill", "#000")
					.style("stroke", "#000")
					.style("stroke-width", "2px");
			}
		},
		Models: Models,
		Views: Views,
		Controllers: Controllers
	});

	function connected (env) {

		window.ENV.isCloud = (env !== 'development');
		console.log('Environment set to "' + env + '"');

		// This function is called when the socket is (re)connected.

		if (!App.initialized) {

			console.log('Application connected.');
			
			// Hack: Keep timestamps updated.
			var trigger = 0;
			setInterval( function () {
				trigger++;
				App.Controllers.Flows.forEach(function (model) {
					model.set('timeTrigger', trigger);
				});
				if (App.Controllers.Flow.current) {
					App.Controllers.Flow.history.forEach(function (model) {
						model.set('timeTrigger', trigger);
					});
				}
			}, 5000);

			App.initialized = true;
			console.log('Application initialized.');

		} else {

			// Reconnected.
			App.interstitial.hide();

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
		App.interstitial.label(message).show();
		
	}

	var socket = io.connect(document.location.hostname);
	var pending = {};
	var current_id = 0;

	App.socket = socket;

	$.extend(socket, {
		request: function (service, request, response) {
			request.id = current_id ++;
			pending[request.id] = response;
			this.emit(service, request);
		}
	});

	socket.on('exec', function (error, response) {
		
		if (typeof pending[response.id] === 'function') {
			pending[response.id](error, response);
			delete pending[response.id];
		}

	});
	socket.on('failure', function (failure) {
		error(failure);
	});
	socket.on('upload', function (response) {
		App.Controllers.Upload.update(response);
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


	// Some templates depend on specific views.
	// Compile templates once all views are loaded.
	App.Views.Flowlet.compile();
				
	console.log('Models, Views, Controllers loaded and assigned.');

	return App;

});