/*
 * Batch Config Controller
 */

define(['core/controllers/runnable-config'], function (RunnableConfigController) {

	var Controller = RunnableConfigController.extend({

		/*
		 * This syntax makes the BatchStatus controller available to this controller.
		 * This allows us to access the Batch model that has already been loaded.
		 *
		 * RunnableConfigController uses this value to do its work. Take a look there.
		 *
		 */
		needs: ['BatchStatus']

	});

	Controller.reopenClass({

		type: 'BatchStatusConfig',
		kind: 'Controller'

	});

	return Controller;

});