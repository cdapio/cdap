

define([
	'lib/text!../partials/modal.html'
	], function (Template) {

		return Em.View.create({
			template: Em.Handlebars.compile(Template),
			classNames: ['modal', 'hide', 'fade'],
			elementId: 'modal-from-dom',
			show: function (title, body, callback, nocancel) {
				this.set('title', title);
				this.set('body', body);

				this.set('nocancel', nocancel );

				this.set('confirmed', function () {
					C.Vw.Modal.hide();
					if (typeof callback === 'function') {
						callback();
					}
				});

				var el = $(this.get('element'));
				el.modal('show');

			},
			hide: function () {
				var el = $(this.get('element'));
				el.modal('hide');
			}

		// Automatically appends to the DOM.
		}).append();
		//
	});