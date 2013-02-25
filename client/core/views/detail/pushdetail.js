
// Push to cloud detail view

define([
	'lib/text!../../partials/pushdetail.html'
	], function (Template) {
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		classNames: ['popup-modal'],
		apiKey: '',
		loading: false,
		destination: null,
		destinations: [],
		reload: function () {

			this.set('loading', true);

			var self = this;
			self.set('destinations', []);

			$.post('/credential', 'apiKey=' + this.get('apiKey'),
				function (result, status) {

				$.getJSON('/destinations', function (result, status) {

					var destinations = [];

					for (var i = 0; i < result.length; i ++) {

						destinations.push({
							id: result[i].vpc_name,
							name: result[i].vpc_label + ' (' + result[i].vpc_name + '.continuuity.net)'
						});

					}

					self.set('destinations', destinations);
					self.set('loading', false);

				});

			});

		}.observes('apiKey'),
		submit: function () {

			this.set("pushing", true);

			// TODO: Complete

		},
		show: function (current) {

			var self = this;

			if (ENV.credential) {
				this.set('apiKey', ENV.credential);
			}

			$(this.get('element')).show();

		},
		hide: function () {

			this.set('current', null);
			$(this.get('element')).hide();

		}
	});
});