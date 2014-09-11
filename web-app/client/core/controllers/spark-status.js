/*
 * Spark Controller
 */

define([], function () {

    var Controller = Em.Controller.extend({

        __STATUS_UPDATE_TIMEOUT: 1000,
        __updateStatusTimeout: null,
        __updateStatus: function(appName, jobName) {
            var model = this.get('model');

            this.HTTP.rest('apps', appName, 'spark', jobName, 'status', function (response) {

                if (!$.isEmptyObject(response)) {
                    model.set('currentState', response.status);
                }

            });
        },

        load: function () {
            var model = this.get('model');
            var self = this;

            var runStatusUpdate = function() {
                self.__updateStatusTimeout = setTimeout(function () {
                    self.__updateStatus(model.app, model.name);

                    runStatusUpdate();
                }, self.__STATUS_UPDATE_TIMEOUT);
            };

            runStatusUpdate();
        },

        unload: function () {
            clearTimeout(this.__updateStatusTimeout);
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
