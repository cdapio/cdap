
define([], function () {

	return function (hostname) {

		var socket = io.connect(hostname);

		socket.on('createTodo', function (todo) {
			// Need to wrap raw JSON objects as Ember objects
			todo = App.Models.Todo.create(todo);
			App.Controllers.Todos.pushObject(todo);
		});

		socket.on('removeTodo', function (todo) {
			App.Controllers.Todos.removeObject(
			App.Controllers.Todos.filterProperty('title', todo.title)[0]);
		});

		socket.on('isDone', function (updated) {
			var todos = App.Controllers.Todos.get('content');
			for (var t = 0; t < updated.length; t ++) {
				var a = null;
				for (var i = 0; i < todos.length; i ++) {
					if (todos[i].title === updated[t].title) {
						todos[i].set('isDone', updated[t].isDone);
					}
				}
			}
		});

		return socket;
	};
});