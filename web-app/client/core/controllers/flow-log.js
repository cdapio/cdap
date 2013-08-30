/*
 * Flow Log Controller
 */

define(['core/controllers/runnable-log'], function (RunnableLogController) {

	var Controller = RunnableLogController.extend({

		init: function () {

			this.set('expectedPath', 'Flow.Log');
			this.set('entityType', C.ENTITY_MAP['FLOW']);

		}

	});

	Controller.reopenClass({
		type: 'FlowLog',
		kind: 'Controller'
	});

	return Controller;

});