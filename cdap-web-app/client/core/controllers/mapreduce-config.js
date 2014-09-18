/*
 * Mapreduce Config Controller
 */

define(['core/controllers/runnable-config'], function (RunnableConfigController) {

	var Controller = RunnableConfigController.extend({

		/*
		 * This syntax makes the MapreduceStatus controller available to this controller.
		 * This allows us to access the Mapreduce model that has already been loaded.
		 *
		 * RunnableConfigController uses this value to do its work. Take a look there.
		 *
		 */
		needs: ['MapreduceStatus']

	});

	Controller.reopenClass({

		type: 'MapreduceStatusConfig',
		kind: 'Controller'

	});

	return Controller;

});