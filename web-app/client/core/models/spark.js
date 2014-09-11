/*
 * Spark Model
 */

define(['core/models/program'], function (Program) {

    var EXPECTED_FIELDS = [
        'name',
        'description'
    ];

    var Model = Program.extend({
        type: 'Spark',
        plural: 'Spark',
        href: function () {
            return '#/spark/' + this.get('id');
        }.property('id'),

        currentState: '',

        init: function () {

            this._super();

            this.set('id', this.get('app') + ':' + this.get('name'));
            this.set('description', this.get('meta') || 'Spark');
            if (this.get('meta')) {
                this.set('startTime', this.get('meta').startTime);
            }

        },

        start: function(http) {
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

        stop: function(http) {
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

            if(this.currentState === 'STARTING' ||
                this.currentState === 'STOPPING') {
                return true;
            }

            return false;

        }.property('currentState')
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