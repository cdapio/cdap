/*
 * Stream Model
 */

define([], function () {

	var Model = Em.Object.extend({
		type: 'Stream',
		plural: 'Streams',
		href: function () {
			return '#/streams/' + this.get('id');
		}.property().cacheable(),

		init: function() {
			this._super();

			this.set('timeseries', Em.Object.create());
			this.set('aggregates', Em.Object.create());

			if (!this.get('id')) {
				this.set('id', this.get('name'));
			}

			this.trackMetric('/collect/bytes/streams/{id}', 'aggregates', 'storage');
			this.trackMetric('/collect/events/streams/{id}', 'aggregates', 'events');

		},
		isSource: true,
		unconsumed: '0',
		storageLabel: '0',
		storageUnit: 'B',

		units: {
			'storage': 'bytes',
			'events': 'number'
		},

		trackMetric: function (name, type, label) {

			name = name.replace(/{id}/, this.get('id'));
			this.get(type)[name] = label;

			return name;

		},

		setMetric: function (label, value) {

			var unit = this.get('units')[label];
			value = C.Util[unit](value);

			this.set(label + 'Label', value[0]);
			this.set(label + 'Units', value[1]);

		}

	});

	Model.reopenClass({
		type: 'Stream',
		kind: 'Model',
		find: function (stream_id, http) {

			var promise = Ember.Deferred.create();

			http.rest('streams', stream_id, function (model, error) {

				model = C.Stream.create(model);
				promise.resolve(model);

			});

			return promise;
		}

	});

	return Model;

});