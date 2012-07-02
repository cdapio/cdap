//
// Flow Controller. Content is an array of flowlets.
//

define([], function () {

	return Em.ArrayProxy.create({
		content: [],
		resource_identifier: null,

		fileQueue: [],
		sendFiles: function (files) {
			
			this.fileQueue = [];
			for (var i = 0; i < files.length; i ++) {
				this.fileQueue.push(files[i]);
			}

			if (files.length > 0) {
				this.sendFile();
			}
		},

		sendFile: function () {

			var file = this.fileQueue.shift();

			if (file === undefined) {
				return;
			}

			var reader = new FileReader();

			reader.onload = function (evt) {

				$('#far-upload-status').html('Loaded file.');

				var xhr = new XMLHttpRequest();
				var uploadProg = xhr.upload || xhr;

				uploadProg.addEventListener('progress', function (e) {

					if (e.type === 'progress') {
						var pct = Math.round((e.loaded / e.total) * 100);
						$('#far-upload-status').html(pct + '% Uploaded...');
					}

				});

				xhr.open('POST', '/upload/' + file.name, true);
				xhr.setRequestHeader("Content-type", "application/octet-stream");
				xhr.send(evt.target.result);

				$('#far-upload-status').html('Uploading...');

				this.processing = true;

			};

			$('#far-upload-status').html('Reading ' + file.name + '...');
			reader.readAsArrayBuffer(file);

		},

		update: function (response) {

			if (response.error) {
				App.Views.Upload.showError(response.error);
				$('#far-upload-status').html(this.welcome_message);
				this.processing = false;

			} else {

				switch (response.step) {
					case 1:
					case 2:
					case 3:
						$('#far-upload-status').html(response.message);
						break;
					case undefined:
						if (response.status === 'initialized') {
							this.resource_identifier = response.resource_identifier;
						}
						$('#far-upload-status').html(response.status);
					break;
					case 5:
						App.Views.Upload.showSuccess(response.status);
						this.processing = false;
						App.Views.Upload.reset();
						this.sendFile();
					break;
					default:
						App.Views.Upload.showError(response.message);
						this.processing = false;
						App.Views.Upload.reset();
						this.sendFile();
				}
			}
		}

	});

});