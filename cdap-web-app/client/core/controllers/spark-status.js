/*
 * Spark Controller
 */

define([], function () {

    var Controller = Em.Controller.extend({

        __STATUS_UPDATE_TIMEOUT: 1000,
        showLogsMessage: false,
        __updateStatusTimeout: null,
        __updateStatus: function (appName, jobName) {
            var model = this.get('model');

            this.HTTP.rest('apps', appName, 'spark', jobName, 'status', function (response) {

                if (!$.isEmptyObject(response)) {
                    model.set('currentState', response.status);
                }

            });
        },
        __updateMetrics: function () {
            this.get('model').getMetricsRequest(this.HTTP);
        },

        load: function () {
            var model = this.get('model');
            var self = this;

            var runPageUpdate = function () {
                self.__updateStatus(model.app, model.name);
                self.__updateMetrics();
            };

            this.__pageUpdateInterval = setInterval(runPageUpdate, this.__STATUS_UPDATE_TIMEOUT);
            runPageUpdate();
        },

        unload: function () {
            clearInterval(this.__pageUpdateInterval);
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

        },

        onCurrentStatusChanged: function () {
            if (this.get("model").get("currentState") === "RUNNING") {
                this.set("showLogsMessage", true);
            }
        }.observes("model.currentState")
    });

    Controller.reopenClass({
        type: 'SparkStatus',
        kind: 'Controller'
    });

    return Controller;

});
