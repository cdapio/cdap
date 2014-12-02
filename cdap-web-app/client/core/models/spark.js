/*
 * Spark Model
 */

define(['core/models/program'], function (Program) {

    var METRICS_PATHS = {
        'system/apps/{{appId}}/spark/{{programId}}/runs/{{runId}}/{{programId}}.BlockManager.memory.remainingMem_MB?aggregate=true': 'blockRemainingMemory',
        'system/apps/{{appId}}/spark/{{programId}}/runs/{{runId}}/{{programId}}.BlockManager.memory.maxMem_MB?aggregate=true': 'blockMaxMemory',
        'system/apps/{{appId}}/spark/{{programId}}/runs/{{runId}}/{{programId}}.BlockManager.memory.memUsed_MB?aggregate=true': 'blockUsedMemory',
        'system/apps/{{appId}}/spark/{{programId}}/runs/{{runId}}/{{programId}}.BlockManager.disk.diskSpaceUsed_MB?aggregate=true': 'blockDiskSpaceUsed',
        'system/apps/{{appId}}/spark/{{programId}}/runs/{{runId}}/{{programId}}.DAGScheduler.job.activeJobs?aggregate=true': 'schedulerActiveJobs',
        'system/apps/{{appId}}/spark/{{programId}}/runs/{{runId}}/{{programId}}.DAGScheduler.job.allJobs?aggregate=true': 'schedulerAllJobs',
        'system/apps/{{appId}}/spark/{{programId}}/runs/{{runId}}/{{programId}}.DAGScheduler.stage.failedStages?aggregate=true': 'schedulerFailedStages',
        'system/apps/{{appId}}/spark/{{programId}}/runs/{{runId}}/{{programId}}.DAGScheduler.stage.runningStages?aggregate=true': 'schedulerRunningStages',
        'system/apps/{{appId}}/spark/{{programId}}/runs/{{runId}}/{{programId}}.DAGScheduler.stage.waitingStages?aggregate=true': 'schedulerWaitingStages'
    };

    var EXPECTED_FIELDS = [
        'name',
        'description'
    ];

    var Model = Program.extend({
        type: 'Spark',
        plural: 'Spark',
        currentState: '',

        href: function () {
            return '#/spark/' + this.get('id');
        }.property('id'),

        init: function () {
            this._super();
            this.set('id', this.get('app') + ':' + this.get('name'));
            this.set('description', this.get('meta') || 'Spark');
            if (this.get('meta')) {
                this.set('startTime', this.get('meta').startTime);
            }

            this.set('metricsData', Em.Object.create({
                blockRemainingMemory: 0,
                blockMaxMemory: 0,
                blockUsedMemory: 0,
                blockDiskSpaceUsed: 0,
                schedulerActiveJobs: 0,
                schedulerAllJobs: 0,
                schedulerFailedStages: 0,
                schedulerRunningStages: 0,
                schedulerWaitingStages: 0
            }));

            //flag for metrics stacked progress
            this.set('metricsUpdated', false);
        },

        start: function (http) {
            var model = this;
            model.set('currentState', 'STARTING');

            http.post('rest', 'apps', this.get('app'), 'spark', model.get('name'), 'start',
                function (response) {

                    if (response.error) {
                        C.Modal.show(response.error, response.message);
                    } else {
                        model.set('lastStarted', new Date().getTime() / 1000);
                    }

                });
        },

        stop: function (http) {
            var model = this;
            model.set('currentState', 'STOPPING');

            http.post('rest', 'apps', this.get('app'), 'spark', model.get('name'), 'stop',
                function (response) {

                    if (response.error) {
                        C.Modal.show(response.error, response.message);
                    } else {
                        model.set('lastStarted', new Date().getTime() / 1000);
                    }

                });
        },

        getStartDate: function () {
            var time = parseInt(this.get('startTime'), 10);
            return new Date(time).toString('MMM d, yyyy');
        }.property('startTime'),

        getStartHours: function () {
            var time = parseInt(this.get('startTime'), 10);
            return new Date(time).toString('hh:mm tt');
        }.property('startTime'),

        context: function () {
            return this.interpolate('apps/{parent}/spark/{id}');
        }.property('app', 'name'),

        interpolate: function (path) {
            return path.replace(/\{parent\}/, this.get('app'))
                .replace(/\{id\}/, this.get('name'));
        },

        startStopDisabled: function () {
            if (this.currentState === 'STARTING' || this.currentState === 'STOPPING') {
                return true;
            }
            return false;
        }.property('currentState'),

        getMetricsRequest: function (http) {
            var self = this;
            var appId = this.get('app');
            var programId = this.get('id');
            http.rest('apps', appId, 'spark', programId, 'runs?limit=1', function (runIdResponse, status) {

                if ((status != 200) || (!runIdResponse.length > 0)) {
                    return;
                }
                var runId = runIdResponse[0]["runid"];
                var paths = [];
                var pathMap = {};

                for (var path in METRICS_PATHS) {
                    var url = S(path).template({'appId': appId, 'programId': programId, 'runId': runId}).s;
                    paths.push(url);
                    pathMap[url] = METRICS_PATHS[path];
                }
                http.post('metrics', paths, function (response, status) {
                    if (!response.result) {
                        return;
                    }
                    var result = response.result;
                    var i = result.length, metric;
                    while (i--) {
                        metric = pathMap[result[i]['path']];
                        if (metric) {
                            var res = result[i]['result'];
                            if (res) {
                                var respData = res['data'];
                                if (respData instanceof Array) {
                                    res['data'] = respData.map(function (entry) {
                                        return entry.value;
                                    });
                                    self.setMetricData(metric, respData);
                                }
                                else {
                                    self.setMetricData(metric, respData);
                                }
                            }
                        }
                        metric = null;
                    }
                    self.set('metricsUpdated', !self.get('metricsUpdated'));
                });
            });
        },

        setMetricData: function (name, value) {
            var metricsData = this.get('metricsData');
            metricsData.set(name, value);
        }
    });

    Model.reopenClass({
        type: 'Spark',
        kind: 'Model',
        find: function (model_id, http) {

            model_id = model_id.split(':');

            var self = this,
                promise = Ember.Deferred.create(),
                app_id = model_id[0],
                spark_id = model_id[1];

            http.rest('apps', app_id, 'spark', spark_id, function (model, error) {

                model = self.transformModel(model);

                model.app = app_id;
                model = C.Spark.create(model);
                model.id = spark_id;
                model.name = spark_id;

                http.rest('apps', app_id, 'spark', spark_id, 'status', function (response) {
                    if (!$.isEmptyObject(response)) {
                        model.set('currentState', response.status);
                        promise.resolve(model);
                    } else {
                        promise.reject('Status not found');
                    }
                });

            });

            return promise;
        },

        transformModel: function (model) {

            var newModel = {};
            for (var i = EXPECTED_FIELDS.length - 1; i >= 0; i--) {
                newModel[EXPECTED_FIELDS[i]] = model[EXPECTED_FIELDS[i]];
            }
            if ('appId' in model || 'applicationId' in model) {
                newModel.appId = model.appId || model.applicationId;
            }
            return newModel;

        }
    });

    return Model;

});