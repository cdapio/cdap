//
// Main entrypoint for client-side application.
//

define(['models', 'views', 'controllers', 'router', 'socket'],
function(Models, Views, Controllers, Router, Socket){
	
	var App = window.App = Ember.Application.create({
		ready: function () {
			this.router = new Router(Views);
			this.socket = new Socket(document.location.hostname, function () {
				
				// This function is called when the socket is (re)connected.

				if (!App.initialized) {
					// Connected and ready.
					App.router.start();
					
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

					App.interstitial = $('#interstitial');
					App.interstitial.label = function (message) {
						$(this).html('<h3>' + message + '</h3>').show();
						return this;
					};
					App.interstitial.loading = function (message) {
						$(this).html((message ? '<h3>' + message + '</h3>' : '') +
							'<img src="/img/loading.gif" />').show();
						return this;
					};

					App.initialized = true;
					
				} else {

					// Reconnected.
					App.interstitial.hide();

				}
				

			}, function (message, args) {

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
				
			});
		}
	});
	
	App.informer = {
		clear: function () {
			$('#informer').html('');
		},
		queue: [],
		show: function (message, style, persist) {

			var div = $('<div class="alert-wrapper" />').append(
				$('<div></div>')
				.addClass('alert').addClass(style).html(message));

			$('#informer').append(div);
			div.fadeIn();

			if (!persist) {

				this.queue.push(div);

				setTimeout(function () {

					var el = App.informer.queue.shift();

					el.animate({
						opacity: 0,
						height: 0
					}, function () {
						el.remove();
					});
					
				}, 4000);
			}
		}
	};

	App.Models = Models;
	App.Views = Views;
	App.Controllers = Controllers;

	// Some templates depend on specific views.
	// Compile templates once all views are loaded.
	App.Views.Flowlet.compile();

});