/*
 * Runnable Config Controller
 * Runnables (Flow, Batch, Procedure) extend this for Config functionality.
 */

define([], function () {

	var Controller = Em.Controller.extend({

		load: function () {

		},

		unload: function () {

		},

		close: function () {

		}

	});

	Controller.reopenClass({

		type: 'RunnableConfig',
		kind: 'Controller'

	});

	return Controller;

});