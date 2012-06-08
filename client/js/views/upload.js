
define([
	'lib/text!../../templates/upload.html'
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template),
		didInsertElement: function () {

			function ignoreDrag(e) {
				e.originalEvent.stopPropagation();
				e.originalEvent.preventDefault();
			}

			function drop (e) {
				ignoreDrag(e);

				if (!this.processing) {
					var dt = e.originalEvent.dataTransfer;
					var files = dt.files;

					if(files.length > 0){
						var file = dt.files[0];
						sendFile(file);
					}

					$('#far-upload-alert').hide();
				}
			}

			function sendFile(file) {

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
			}

			$('#far-upload')
				.bind('dragenter', ignoreDrag)
				.bind('dragover', ignoreDrag)
				.bind('drop', drop);

			this.welcome_message = $('#far-upload-status').html();
			$('#far-upload-alert').hide();
		},
		cancel: function () {
			App.router.set('location', '/');
		},
		showError: function (message) {
			console.log(message);
			$('#far-upload-alert').html('Error: ' + message).show();
		},
		showSuccess: function (message) {
			$('#far-upload-alert').removeClass('alert-error')
				.addClass('alert-success').html('Flow uploaded, verified, and deployed!').show();
		},
		identifier: null,
		update: function (response) {
			if (response.error) {
				this.showError(response.error);
				$('#far-upload-status').html(this.welcome_message);
				this.processing = false;

			} else {
				switch (response.step) {
					case 1:
					case 2:
					case 3:
					case undefined:
						$('#far-upload-status').html(response.status);
					break;
					case 5:
						this.showSuccess(response.status);
						this.processing = false;
						$('#far-upload-status').html(this.welcome_message);
					break;
					default:
						this.showError(response.status);
						this.processing = false;
						$('#far-upload-status').html(this.welcome_message);
				}
			}
		}
	});

});