/*
 * Spark Controller
 */

define([], function () {

    var Controller = Em.Controller.extend({

        load: function () {

        },

        unload: function () {

        },

        /**
         * Action handlers from the View
         */
        exec: function () {

            var model = this.get('model'),
                action = model.get('defaultAction');

            if (action && action.toLowerCase() in model) {
                model[action.toLowerCase()](this.HTTP);
            }

        },

        config: function () {

            var self = this,
                model = this.get('model');

            this.transitionToRoute('SparkStatus.Config');

        }

    });

    Controller.reopenClass({
        type: 'SparkStatus',
        kind: 'Controller'
    });

    return Controller;

});
