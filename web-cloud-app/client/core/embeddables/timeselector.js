/*
 * Time Selector Embeddable
 */

define([
	'lib/text!../partials/timeselector.html'
	], function (Template) {

	var Embeddable = Em.View.extend({
		template: Em.Handlebars.compile(Template),
		setTime: function (time) {

			time = parseInt(time, 10);

			C.setTimeRange(time);

			var element = $(this.get('element'));

			element.find('img').show();
			element.find('button').hide();

			setTimeout(function () {

				element.find('img').hide();
				element.find('button').show();

			}, 1000);

		}
	});

	Embeddable.reopenClass({

		type: 'TimeSelector',
		kind: 'Embeddable'

	});

	return Embeddable;

});