//
// Main entrypoint for client-side application.
//

define(['models', 'views', 'controllers', 'router', 'socket'],
function(Models, Views, Controllers, Router, Socket){
	
	var App = window.App = Ember.Application.create({
		ready: function () {
			this.router = new Router(Views);
			this.socket = new Socket(document.location.hostname, function () {
				
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

			});
		}
	});
	
	App.Models = Models;
	App.Views = Views;
	App.Controllers = Controllers;

});