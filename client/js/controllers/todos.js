
define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		createTodo: function(title) {
			var todo = App.Models.Todo.create({ title: title });
			App.socket.emit('createTodo', todo);
		},

		clearCompletedTodos: function() {
			this.filterProperty('isDone', true).forEach(function (todo) {
				App.socket.emit('removeTodo', todo);
			}, this);
		},

		remaining: function() {
			App.socket.emit('isDone', this.get('content'));
			return this.filterProperty('isDone', false).get('length');
		}.property('@each.isDone'),

		allAreDone: function(key, value) {
			if (value !== undefined) {
				this.setEach('isDone', value);
				return value;
			} else {
				return !!this.get('length') && this.everyProperty('isDone', true);
			}
		}.property('@each.isDone')
	});

});