
define([], function () {
	return Em.TextField.extend({
		insertNewline: function() {
			var value = this.get('value');
			if (value) {
				App.Controllers.Todos.createTodo(value);
				this.set('value', '');
			}
		}
	});
});