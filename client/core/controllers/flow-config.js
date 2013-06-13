/*
 * Flow Stream Controller
 */

define(['core/controllers/runnable-config'], function (RunnableConfigController) {

	var Controller = RunnableConfigController.extend({

		/*
		 * This syntax makes the FlowStatus controller available to this controller.
		 * This allows us to access the flow model that has already been loaded.
		 */
		needs: ['FlowStatus'],

		load: function () {

			var flow = this.get('controllers').get('FlowStatus').get('model');
			this.set('model', flow);

		},

		unload: function () {

		},

		close: function () {

			var model = this.get('controllers').get('FlowStatus').get('model');

			/*
			 * HAX: The URL route needs the ID of a flow to be app_id:flow_id.
			 * However, Ember is smart enough to not reload the parent controller.
			 * Therefore, the "correct" ID is preserved on the parent controller's model.
			 */

			if (model.id && model.id.indexOf(':') === -1) {
				model.id = model.app + ':' + model.id;
			}

			this.transitionToRoute('FlowStatus', model);

		}

	});

	Controller.reopenClass({

		type: 'FlowStatusConfig',
		kind: 'Controller'

	});

	return Controller;

});