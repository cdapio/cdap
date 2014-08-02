/*
 * Service Config Controller
 */

define(['core/controllers/runnable-config'], function (RunnableConfigController) {

	var Controller = RunnableConfigController.extend({

		/*
		 * This syntax makes the Service controller available to this controller.
		 * This allows us to access the service model that has already been loaded.
		 *
		 * RunnableConfigController uses this value to do its work. Take a look there.
		 *
		 */
		 needs: ['Service'],
		 init: function () {
       this.set('expectedPath', 'Service.Config');
     }

	});

	Controller.reopenClass({

		type: 'ServiceConfig',
		kind: 'Controller'

	});

	return Controller;

});