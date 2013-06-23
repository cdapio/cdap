/*
 * Stream Injector Embeddable
 */

define([], function () {

	var Embeddable = Em.TextField.extend({
		valueBinding: 'controller.injectValue',
		insertNewline: function() {
			var value = this.get('value');
			if (value) {
				this.get('controller').inject(value);
			}
			this.set('value', '');
		},
		didInsertElement: function () {

			$(this.get('element')).css({width: '222px'});

		}
	});

	Embeddable.reopenClass({

		type: 'Injector',
		kind: 'Embeddable'

	});

	return Embeddable;

});