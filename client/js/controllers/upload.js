//
// Flow Controller. Content is an array of flowlets.
//

define([], function () {

	return Em.ArrayProxy.create({
		content: [],
		resource_identifier: null,

		sendFile: function (file) {

			var reader = new FileReader();

			reader.onload = function (evt) {

				var xhr = new XMLHttpRequest();
				if (xhr.upload) {
					xhr.upload.onprogress = function () {
						
					};
				}
				xhr.open('POST', '/upload', true);
				xhr.setRequestHeader("Content-type", "application/octet-stream");
				xhr.send(evt.target.result);

				$('#far-upload-status').html('Uploading...');
				this.processing = true;

			};
			$('#far-upload-status').html('Loading...');
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
					break;
					default:
						App.Views.Upload.showError(response.message);
						this.processing = false;
						App.Views.Upload.reset();
				}
			}
		}

	});

});