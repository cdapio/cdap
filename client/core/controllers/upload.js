//
// Flow Controller. Content is an array of flowlets.
//

define([], function () {

	return Em.ArrayProxy.create({
		content: [],
		resource_identifier: null,
		fileQueue: [],
		entityType: null,
		sendFiles: function (files, type) {
			
			this.set('entityType', type);

			this.fileQueue = [];
			for (var i = 0; i < files.length; i ++) {
				this.fileQueue.push(files[i]);
			}

			if (files.length > 0) {
				this.sendFile();
			}
		},

		sendFile: function () {

			var applicationId = C.Ctl.Application.current.id;

			var file = this.fileQueue.shift();
			if (file === undefined) {
				window.location.reload();
				return;
			}

			var xhr = new XMLHttpRequest();
			var uploadProg = xhr.upload || xhr;

			uploadProg.addEventListener('progress', function (e) {

				if (e.type === 'progress') {
					var pct = Math.round((e.loaded / e.total) * 100);
					$('#far-upload-status').html(pct + '% Uploaded...');
				}

			});

			xhr.open('POST', '/upload/' + this.get('entityType') + '/' +
				applicationId + '/' + file.name, true);
			xhr.setRequestHeader("Content-type", "application/octet-stream");
			xhr.send(file);

		},
		message: '',
		update: function (response) {

			if (response.error) {
				C.Vw.Informer.show(response.error, 'alert-error');
				this.processing = false;

			} else {

				switch (response.step) {
					case 1:
					case 2:
					case 3:
						this.set('message', response.message);
						break;
					case undefined:
						if (response.status === 'initialized') {
							this.resource_identifier = response.resource_identifier;
						}
						this.set('message', response.status);
					break;
					case 5:
						this.set('message', '');
						this.processing = false;
						this.sendFile();
					break;
					default:
						this.set('message', '');
						this.processing = false;
						this.set('warningMessage', response.message);

						C.Vw.Modal.show(
							"Deployment Error",
							response.message, function () {
								window.location.reload();
							});

				}
			}
		}

	});

});