
define([], function () {

	return Ember.TextField.extend({
		valueBinding: 'parentView.injectValue',
		insertNewline: function() {
			var value = this.get('value');
			if (value) {
				this.get('parentView').inject();
			}
			this.set('value', '');
		},
		didInsertElement: function () {

			$(this.get('element')).css({width: '222px'});

		}
	});
	
});