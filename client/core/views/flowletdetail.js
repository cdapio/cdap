
// Flowlet detail view

define([
	'lib/text!../partials/flowletdetail.html'
	], function (Template) {
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		__timeout: null,
		show: function (current) {

			var self = this;

			this.set('controller', Em.Object.create({
				current: current,
				getStats: function () {

					C.get.apply(this.get('current'), this.get('current').getUpdateRequest());

					self.__timeout = setTimeout(function () {
						self.get('controller').getStats();
					}, 1000);

				}
			}));

			this.get('controller').getStats();
			$('#flowlet-container').show();

			this.select('processed');

		},
		select: function (event) {

			var tabName;

			if (typeof event === 'string') {
				tabName = event;
			} else {
				tabName = $(event.target).attr('tab-name');
			}

			$('#flowlet-popup-input').hide();
			$('#flowlet-popup-processed').hide();
			$('#flowlet-popup-output').hide();

			$('#flowlet-popup-' + tabName).show();

		},
		close: function () {
			$('#flowlet-container').hide();
			this.set('current', null);

			clearTimeout(this.__timeout);

		}
	});
});