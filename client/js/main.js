//
// Main entrypoint for client-side application.
//

define(['models', 'views', 'controllers', 'router', 'socket'],
function(Models, Views, Controllers, Router, Socket){

	// Create an Ember application.

	var App = window.App = Ember.Application.create({
		ready: function () {
			(this.router = new Router(Views)).start();
			this.socket = new Socket(document.location.hostname);
		}
	});
	
	App.Models = Models;
	App.Views = Views;
	App.Controllers = Controllers;

});