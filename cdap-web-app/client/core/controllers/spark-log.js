/*
 * Spark Log Controller
 */

define(['core/controllers/runnable-log'], function (RunnableLogController) {

    var Controller = RunnableLogController.extend({

        init: function () {

            this.set('expectedPath', 'Spark.Log');

        }

    });

    Controller.reopenClass({
        type: 'SparkLog',
        kind: 'Controller'
    });

    return Controller;

});