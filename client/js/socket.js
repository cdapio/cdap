
define([], function () {

	return function (hostname, connected, error) {

		var socket = io.connect(hostname);
		var pending = {};
		var current_id = 0;

		socket.on('exec', function (response) {
			
			if (typeof pending[response.id] === 'function') {
				pending[response.id](response);
				delete pending[response.id];
			}

		});

		socket.on('failure', function (failure) {
			error(failure);
		});

		socket.on('upload', function (response) {

			App.Views.Upload.update(response);

		});

		socket.on('connect', connected);
		socket.on('error', function () {
			error('Error', arguments);
		});

		socket.on('connect_failed', function () {
			error('Connection failed.', arguments);
		});
		socket.on('reconnect_failed', function () {
			error('Reconnect failed.', arguments);
		});
		socket.on('reconnect', function () {
			error('Reconnected.', arguments);
		});
		socket.on('reconnecting', function (timeout, attempt) {
			error('Reconnecting. Attempt ' + attempt + '.', arguments);
		});

		$.extend(socket, {
			request: function (service, request, response) {
				request.id = current_id ++;
				pending[request.id] = response;
				this.emit(service, request);
			}
		});

		return socket;
	};
});