/*
 * Stream Injector Embeddable
 */

define([], function () {

	var Embeddable = Em.TextArea.extend({
		valueBinding: 'controller.injectValue',
		elementId: 'flow-injector-input',
		didInsertElement: function () {

			window.tabOverride.set(this.get('element'));

			var controller = this.get('controller');
			$(this.get('element')).keydown(function (e) {
				if (e.keyCode === 13 && controller.get('injectOnEnter')) {
					controller.inject();
					e.preventDefault();
					return false;
				}
			});

		}
	});

	Embeddable.reopenClass({

		type: 'Injector',
		kind: 'Embeddable'

	});

	return Embeddable;

});