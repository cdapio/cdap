/*
 * Mapreduce Log Controller
 */

define(['core/controllers/runnable-log'], function (RunnableLogController) {

	var Controller = RunnableLogController.extend({

		init: function () {

			this.set('expectedPath', 'Mapreduce.Log');
			this.set('entityType', 'mapreduce');
		}

	});

	Controller.reopenClass({
		type: 'MapreduceLog',
		kind: 'Controller'
	});

	return Controller;

});