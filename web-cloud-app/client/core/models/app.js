/*
 * App Model
 */

define([], function () {

	var Model = Em.Object.extend({

		href: function () {
			return '#/apps/' + this.get('id');
		}.property(),
		metricData: null,
		metricNames: null,
		__loadingData: false,
		init: function() {
			this._super();

			this.set('timeseries', Em.Object.create());
			this.set('metrics', []);

			this.set('counts', {
				Stream: 0,
				Flow: 0,
				Batch: 0,
				Dataset: 0,
				Query: 0
			});

		},
		addMetricName: function (name) {

			name = name.replace(/{id}/, this.get('id'));
			this.get('metrics').push(name);

			return name;

		},
		update: function (http) {

			return this.get('metrics').slice(0);

		}

			/*

			var self = this;

			var id = this.get('id'),
				start = C.__timeRange * -1;

			var metrics = [];
			var metricNames = this.get('metricNames');

			for (var name in metricNames) {
				if (metricNames[name] === 1) {
					metrics.push(name);
				}
			}
			if (!metrics.length) {

				//C.debug('Not tracking any metrics for Application ' + id);

				this.set('__loadingData', false);
				return;
			}

			return ['monitor', {
				method: 'getTimeSeries',
				params: [id, null, metrics, start, undefined, 'APPLICATION_LEVEL']
			}, function (error, response, params) {

				if (self.get('isDestroyed')) {
					return;
				}
				if (!response.params) {
					return;
				}

				var data, points = response.params.points,
					latest = response.params.latest;

				for (var metric in points) {
					data = points[metric];

					var k = data.length;
					while(k --) {
						data[k] = data[k].value;
					}

					metric = metric.replace(/\./g, '');
					self.get('metricData').set(metric, data);

				}

				if (typeof done === 'function') {
					done();
				}

			}];
		}
		*/
	});

	Model.reopenClass({
		type: 'App',
		kind: 'Model',
		find: function(model_id, http) {

			var promise = Ember.Deferred.create();

			http.rest('apps', model_id, function (model, error) {

				model = C.App.create(model);
				promise.resolve(model);

			});

			return promise;
		}
	});

	return Model;

});