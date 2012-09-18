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

			var xhr = new XMLHttpRequest();
			var uploadProg = xhr.upload || xhr;

			uploadProg.addEventListener('progress', function (e) {

				if (e.type === 'progress') {
					var pct = Math.round((e.loaded / e.total) * 100);
					$('#far-upload-status').html(pct + '% Uploaded...');
				}

			});

			// Safari does not support nor require FileReader to upload.
			
			if ($.browser.safari &&
				/chrome/.test(navigator.userAgent.toLowerCase())) {

				var reader = new FileReader();

				reader.onload = function (evt) {

					$('#far-upload-status').html('Loaded file.');

					xhr.open('POST', '/upload/' + file.name, true);
					xhr.setRequestHeader("Content-type", "application/octet-stream");
					xhr.send(evt.target.result);

					$('#far-upload-status').html('Uploading...');

					this.processing = true;

				};

				$('#far-upload-status').html('Reading file...');
				reader.readAsArrayBuffer(file);

			} else {
				xhr.open('POST', '/upload/' + file.name, true);
				xhr.setRequestHeader("Content-type", "application/octet-stream");
				xhr.send(file);
			}

		},

		update: function (response) {

			if (response.error) {
				App.Views.Informer.show(response.error, 'alert-error');
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
						App.Views.Informer.show('Success! The FAR was uploaded, and flows deployed.', 'alert-success');
						this.processing = false;
						App.router.applicationController.view.resetUpload();
						this.sendFile();
					break;
					default:
						App.Views.Informer.show(response.message, 'alert-error');
						this.processing = false;
						App.router.applicationController.view.resetUpload();
						this.sendFile();
				}
			}
		}

	});

});