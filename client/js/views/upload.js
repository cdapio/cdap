
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

				if (!App.Controllers.Upload.processing) {
					var dt = e.originalEvent.dataTransfer;
					var files = dt.files;

					if(files.length > 0){
						var file = dt.files[0];
						App.Controllers.Upload.sendFile(file);
					}

					$('#far-upload-alert').hide();
				}
			}

			$('#far-upload')
				.bind('dragenter', ignoreDrag)
				.bind('dragover', ignoreDrag)
				.bind('drop', drop);

			this.welcome_message = $('#far-upload-status').html();
			$('#far-upload-alert').hide();
		},
		reset: function () {
			$('#far-upload-status').html(this.welcome_message);
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
				.addClass('alert-success').html('Success! Go to <a href="#">All Flows</a>').show();

			// $('#object-list').slideDown('slow');
		}
	});

});