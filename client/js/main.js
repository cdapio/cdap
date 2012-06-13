//
// Main entrypoint for client-side application.
//

define(['models', 'views', 'controllers', 'router', 'socket'],
function(Models, Views, Controllers, Router, Socket){
	
	var App = window.App = Ember.Application.create({
		ready: function () {
			this.router = new Router(Views);
			this.socket = new Socket(document.location.hostname, function () {
				
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
					}, 100);

					App.interstitial = $('#interstitial');
					App.interstitial.label = function (message) {
						$(this).html('<h3>' + message + '</h3>').show();
						return this;
					};
					App.interstitial.loading = function () {
						$(this).html('<img src="/img/loading.gif" />').show();
						return this;
					};

					App.initialized = true;
					
				} else {

					// Reconnected.
					App.interstitial.hide();

				}
				

			}, function (message, args) {

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
	
	App.Models = Models;
	App.Views = Views;
	App.Controllers = Controllers;

});