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
        schedulerFailedLineSize: 'width: 33.3%',
        schedulerRunningLineSize: 'width: 33.3%',
        schedulerWaitingLineSize: 'width: 33.3%',

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

            this.set("metricsData", Em.Object.create({
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
                    //resize stage status line
                    self.rescaleLineSizes()
                });
            });
        },

        setMetricData: function (name, value) {
            var metricsData = this.get('metricsData');
            metricsData.set(name, value);
        },

        rescaleLineSizes: function () {
            var model = this;
            var metricsData = this.get('metricsData');
            var failedStages = metricsData.schedulerFailedStages;
            var runningStages = metricsData.schedulerRunningStages;
            var waitingStages = metricsData.schedulerWaitingStages;
            var totalStages = failedStages + runningStages + waitingStages;
            var ptrn = S('width: {{w}}%;');
            //default values if totalStages == 0
            var runningRatio = 33.3,
                failedRatio = 33.3,
                waitingRatio = 33.3;
            if (totalStages != 0) {
                //if some of stages have zero value then we have to assign 3% (min ratio) to this stage ratio and
                // reduce ratio of another stages
                checkForZero(runningStages, failedStages, waitingStages);
                runningRatio = getRatio(runningStages);
                failedRatio = getRatio(failedStages);
                waitingRatio = getRatio(waitingStages);
            }
            model.set('schedulerRunningLineSize', compilePtrn(runningRatio));
            model.set('schedulerFailedLineSize', compilePtrn(failedRatio));
            model.set('schedulerWaitingLineSize', compilePtrn(waitingRatio));

            /*
             In stages status line blocks we use minimum width 3% so '0' value can be displayed correct.
             How recalculation works:
             For example,
             if runningStage = 5, failedRatio = 5, waitingRatio = 0
             then we have ratio
             runningRatio = 50%
             failedRatio = 50%
             waitingRatio = 3% (because minimum ratio must be 3% for correct drawing)
             so, we have total sum 103% that can broke our drawing
             To resolve this we can reduce ration of running and failed stage to 48.5% (50% - 3%/2)
             and we can draw correct proportions of stage blocks.
             Here we try to find zero values, and each zero will increase totalStages for 3% of it value

             x = 5
             y = 5
             z = 0
             minPercent = 3
             zeroMultiplier = 1
             totalStages = x + y + z = 10
             totalStages += totalStages / (100 - minPercent*zeroMultiplier) * minPercent
             (numbers: totalStages += 10 / (100 - 3*1) * 3 = 10,309)
             so now we can use formula for percent ratio
             ratioX = x / (totalStages / 100) = 48.5
             ratioY = y / (totalStages / 100) = 48.5
             ratioZ = 3 (as minimum)

             If we have several zero values
             x = 2
             y = 0
             z = 0
             minPercent = 3
             zeroMultiplier = 1
             totalStages = x + y + z = 2
             sum for each zero
             totalStages += totalStages / (100 - minPercent*zeroMultiplier) * minPercent = 2,063
             zeroMultiplier++
             totalStages += totalStages / (100 - minPercent*zeroMultiplier) * minPercent = 2,1341
             (numbers: totalStages += 2,063 / (100 - 3*2) * 3 = 2,1341)
             ratioX = x / (totalStages / 100) = 94
             ratioY = 3 (as minimum)
             ratioZ = 3 (as minimum)
             */
            function checkForZero() {
                var multiplier = 1;
                var minPercent = 3;
                for (var i = 0; i < arguments.length; i++) {
                    if (arguments[i] === 0) {
                        //append extra width to status line that we can draw 3% min width of status block
                        totalStages += (totalStages / (100 - minPercent * multiplier++)) * minPercent;
                    }
                }
            }

            function getRatio(val) {
                if (val === 0) {
                    //minimum 3% status line width
                    return 3;
                }
                return val / (totalStages / 100);
            }

            function compilePtrn(val) {
                return ptrn.template({'w': val.toString()}).s;
            }
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