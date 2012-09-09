
define([], function () {

	return Ember.TextField.extend({
		valueBinding: 'App.Views.Flowlet.payload',
		insertNewline: function() {
			var value = this.get('value');
			if (value) {
				App.Views.Flowlet.inject();
			}
		}
	});
	
});