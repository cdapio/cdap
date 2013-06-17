/*
 * Runnable Log Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({

		load: function () {

			function resize () {
				$('#logView').css({height: ($(window).height() - 240) + 'px'});
			}

			resize();

			var goneOver = false;
			var app = this.get('model').app;
			var id = this.get('model').name;
			var self = this;

			function logInterval () {

				if (C.currentPath !== self.get('expectedPath')) {
					clearInterval(self.interval);
					return;
				}

				resize();

				C.get('monitor', {
					method: 'getLog',
					params: [app, id, 1024 * 10]
				}, function (error, response) {

					if (C.currentPath !== self.get('expectedPath')) {
						clearInterval(self.interval);
						return;
					}

					if (error) {

						response = JSON.stringify(error);

					} else {

						var items = response.params;
						if (items) {
							for (var i = 0; i < items.length; i ++) {
								items[i] = '<code>' + items[i] + '</code>';
							}
							response = items.join('');

							if (items.length === 0) {
								response = '[ No Log Messages ]';
							}
						} else {
							response = '[ No Log Messages ]';
						}

					}

					$('#logView').html(response);
					var textarea = $('#logView');

					setTimeout(function () {

						// Content exceeds height
						if (textarea[0].scrollHeight > textarea.height()) {

							if (!goneOver) {
								textarea.scrollTop(textarea[0].scrollHeight);
								goneOver = true;
							}

							// Scrolled off the bottom
							if (textarea[0].scrollTop + textarea.height() > textarea[0].scrollHeight) {
								textarea.scrollTop(textarea[0].scrollHeight);
							}

						}

					}, C.EMBEDDABLE_DELAY);

				});
			}

			setTimeout(function () {
				logInterval();
			}, C.EMBEDDABLE_DELAY);

			this.interval = setInterval(logInterval, C.POLLING_INTERVAL);

		},

		interval: null,
		unload: function () {

			clearInterval(this.interval);

		}

	});

	Controller.reopenClass({
		type: 'RunnableLog',
		kind: 'Controller'
	});

	return Controller;

});