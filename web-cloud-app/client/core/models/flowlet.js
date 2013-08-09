/*
 * Flowlet Model
 */

define([], function () {

	var Model = Em.Object.extend({
		metricData: null,
		metricNames: null,
		__loadingData: false,
		elementId: function () {

			return 'flowlet' + this.get('id');

		}.property().cacheable(),
		init: function() {

			this._super();

			this.set('timeseries', Em.Object.create());
			this.set('aggregates', Em.Object.create());
			this.set('rates', Em.Object.create());

			this.set('id', this.get('name'));

		},

		interpolate: function (path) {

			return path.replace(/\{app\}/, this.get('app'))
				.replace(/\{flow\}/, this.get('flow'))
				.replace(/\{id\}/, this.get('id'));

		},

		trackMetric: function (path, kind, label) {

			this.get(kind).set(path = this.interpolate(path), label || []);
			return path;

		},

		units: {
			'events': 'number'
		},

		setMetric: function (label, value) {

			var unit = this.get('units')[label];
			value = C.Util[unit](value);

			this.set(label + 'Label', value[0]);
			this.set(label + 'Units', value[1]);

		},

		clearMetrics: function () {

			this.set('timeseries', Em.Object.create());
			this.set('aggregates', Em.Object.create());
			this.set('rates', Em.Object.create());

		},

		plural: function () {
			return this.instances === 1 ? '' : 's';
		}.property('instances'),
		doubleCount: function () {
			return 'Add ' + this.instances;
		}.property().cacheable(false),
		fitCount: function () {
			return 'No Change';
		}.property().cacheable(false)
	});

	Model.reopenClass({
		type: 'Flowlet',
		kind: 'Model',
		find: function (flowlet_id, http) {

			/*
			 * We will use this Flowlet and its ID to find the
			 * Flowlet model in the parent controller.
			 * See FlowletController.load()
			 */

			return C.Flowlet.create({
				'name': flowlet_id
			});

		}
	});

	return Model;

});