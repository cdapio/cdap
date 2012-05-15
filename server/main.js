var express = require('express')
  , app = express.createServer()
  , io = require('socket.io').listen(app);

app.use(express.static(__dirname + '/../build/'));

app.listen(8081);

app.get('/', function (req, res) {
  res.sendfile(__dirname + '/index.html');
});

var todos = {};
var listeners = [];

io.sockets.on('connection', function (socket) {
  listeners.push(socket);

  for (var t in todos) {
    socket.emit('createTodo', todos[t]);
  }
  socket.on('createTodo', function (data) {
    todos[data.title] = data;
    for (var i = 0 ; i < listeners.length; i ++) {
      listeners[i].emit('createTodo', data);
    }
  });
  socket.on('removeTodo', function (data) {
    delete todos[data.title];
    for (var i = 0 ; i < listeners.length; i ++) {
      listeners[i].emit('removeTodo', data);
    }
  });
  socket.on('isDone', function (data) {
    for (var i = 0 ; i < listeners.length; i ++) {
      listeners[i].emit('isDone', data);
    }
  });
});
