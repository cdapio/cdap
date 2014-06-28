/*
 * Stream Model
 */

define(['core/models/element'], function (Element) {

	var Model = Element.extend({
		type: 'Stream',
		plural: 'Streams',
		href: function () {
			return '#/streams/' + this.get('id');
		}.property().cacheable(),

		init: function() {
			this._super();

			if (!this.get('id')) {
				this.set('id', this.get('name'));
			}

			this.trackMetric('/reactor/streams/{id}/collect.bytes', 'aggregates', 'storage');
			this.trackMetric('/reactor/streams/{id}/collect.events', 'aggregates', 'events');

		},
		isSource: true,
		unconsumed: '0',
		storageLabel: '0',
		storageUnit: 'B',

		interpolate: function (path) {

			return path.replace(/\{id\}/, this.get('id'));

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