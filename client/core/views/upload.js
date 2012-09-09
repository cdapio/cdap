
define([
	'lib/text!../partials/upload.html'
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
					
					App.Controllers.Upload.sendFiles(dt.files);
	
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
		}
	});

});