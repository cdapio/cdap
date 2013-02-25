
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
		message: null,
		reload: function () {

//			this.set('loading', true);

			var self = this;
			self.set('destinations', []);
			self.set('message', null);

			self.set('message', 'There was a problem connecting to Continuuity. Please check your internet connection.');

			return;


			$.post('/credential', 'apiKey=' + this.get('apiKey'),
				function (result, status) {

				$.getJSON('/destinations', function (result, status) {

					if (result === 'network') {

						self.set('message', 'There was a problem connecting to Continuuity. Please check your internet connection.');

					} else {

						self.set('message', 'There are no destinations available. Make sure your API Key is correct.<br /><br />If you have not configured any clouds, you can do so on your <a target="_blank" href="https://accounts.continuuity.com/">Account Home</a> page.</div>');

						var destinations = [];

						for (var i = 0; i < result.length; i ++) {

							destinations.push({
								id: result[i].vpc_name,
								name: result[i].vpc_label + ' (' + result[i].vpc_name + '.continuuity.net)'
							});

						}

						self.set('destinations', destinations);

					}

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