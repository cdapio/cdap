/*
 * Spark Config Controller
 */

define(['core/controllers/runnable-config'], function (RunnableConfigController) {

    var Controller = RunnableConfigController.extend({

        /*
         * This syntax makes the SparkStatus controller available to this controller.
         * This allows us to access the Spark model that has already been loaded.
         *
         * RunnableConfigController uses this value to do its work. Take a look there.
         *
         */
        needs: ['SparkStatus']

    });

    Controller.reopenClass({

        type: 'SparkStatusConfig',
        kind: 'Controller'

    });

    return Controller;

});