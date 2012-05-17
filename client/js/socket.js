
define([], function () {

	return function (hostname, connected) {

		var socket = io.connect(hostname);
		var pending = {};
		var current_id = 0;

		socket.on('exec', function (response) {
			
			if (typeof pending[response.id] == 'function') {
				pending[response.id](response);
				delete pending[response.id];
			}

		});

		socket.on('connect', connected);

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