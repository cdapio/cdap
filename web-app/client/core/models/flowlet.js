/*
 * Flowlet Model
 */

define(['core/models/element'], function (Element) {

	var Model = Element.extend({
		type: 'Flow',
		href: function () {
			return '#/flows/' + this.get('app') + ':' + this.get('flow') + '/flowlets/' + this.get('id');
		}.property('id'),
		metricData: null,
		metricNames: null,
		__loadingData: false,
		elementId: function () {

			return 'flowlet' + this.get('id');

		}.property().cacheable(),
		init: function() {

			this._super();

			this.set('id', this.get('name'));
			this.set('description', 'Flowlet');

		},

		context: function () {
			return this.interpolate('/apps/{app}/flows/{flow}/flowlets/{id}');

		}.property('app', 'flow', 'id'),

		interpolate: function (path) {
			return path.replace(/\{app\}/, this.get('app'))
				.replace(/\{flow\}/, this.get('flow'))
				.replace(/\{id\}/, this.get('id'));

		},

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