
define([], function () {

	return Ember.TextField.extend({
		valueBinding: 'parentView.payload',
		insertNewline: function() {
			var value = this.get('value');
			if (value) {
				this.get('parentView').inject();
			}
		}
	});
	
});