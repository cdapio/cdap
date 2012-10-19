
define([], function () {

	return Ember.TextField.extend({
		insertNewline: function() {
			var value = this.get('value');
			if (value) {
				this.get('parentView').submit();
			}
		}
	});
	
});