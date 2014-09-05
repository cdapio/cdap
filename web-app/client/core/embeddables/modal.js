/*
 * Modal Dialogue Embeddable
 */

define([
	'core/lib/text!core/partials/modal.html'
	], function (Template) {

		var Embeddable = Em.View.extend({

			template: Em.Handlebars.compile(Template),
			classNames: ['modal', 'hide', 'fade'],
			elementId: 'modal-from-dom',

			didInsertElement: function () {
				var self = this;

				// Hide on escape.
				$(document).keyup(function (e) {
					if(e.which == 27) {
						self.hide();
					}
				});
			},

			show: function (title, body, callback, nocancel) {
				this.set('title', title);
				this.set('body', body);

				if (!callback) {
					nocancel = true;
				}

				this.set('nocancel', nocancel );

				var self = this;

				this.set('confirmed', function () {
					self.hide();
					if (typeof callback === 'function') {
						callback();
					}
				});

				var el = $(this.get('element'));
				el.modal({
					show: true,
					backdrop: "static"
				});

			},

			hide: function (callback) {

				var el = $(this.get('element'));

				if (typeof callback === "function") {
					el.on('hidden', function () {
						el.off('hidden');
						callback();
					});
				}

				el.modal('hide');

			}

		});

		Embeddable.reopenClass({
			type: 'Modal',
			kind: 'Embeddable'
		});

		return Embeddable;

	});