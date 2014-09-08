/*
 * Spark History Controller
 */

define([], function () {

    var Controller = Ember.Controller.extend({

        runs: Ember.ArrayProxy.create({
            content: []
        }),

        elements: Em.Object.create(),
        __HISTORY_UPDATE_TIMEOUT: 1000,

        __updateHistoryTimeout: null,
        __updateHistory: function(appName, jobName) {
            this.HTTP.rest('apps', appName, 'spark', jobName, 'history', function (response) {

                if (response) {
                    var history = response;

                    self.runs.pushObjects(
                        history.map(function (item) {
                            return C.Run.create(history[i]);
                        })
                    );
                }

            });
        },

        load: function () {
            var model = this.get('model');
            var self = this;

            var runHistoryUpdate = function() {
                this.__updateHistoryTimeout = setTimeout(function () {
                    this.__updateHistory(model.app, model.name);

                    runHistoryUpdate();
                }, this.__HISTORY_UPDATE_TIMEOUT);
            };

            runHistoryUpdate();

        },

        unload: function () {

            clearTimeout(this.__updateHistoryTimeout);
            this.get('runs').set('content', []);

        }
    });

    Controller.reopenClass({
        type: 'SparkHistory',
        kind: 'Controller'
    });

    return Controller;

});