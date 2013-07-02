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

			this.set('id', this.get('name'));

		},

		trackMetric: function (name, type, label) {

			name = name.replace(/{parent}/, this.get('flow'));
			name = name.replace(/{id}/, this.get('id'));
			this.get(type)[name] = label;

			return name;

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