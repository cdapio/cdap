
define([
	'lib/text!../partials/upload.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile('<div id="upload-dropzone" class="drop-zone">' +
			'<div id="far-upload-status">Drop JAR or <a href="#">Browse</a></div></div>'),
		didInsertElement: function () {

			function ignoreDrag(e) {
				e.originalEvent.stopPropagation();
				e.originalEvent.preventDefault();
			}

			function drop (e) {
				ignoreDrag(e);

				if (!C.Ctl.Upload.processing) {
					var dt = e.originalEvent.dataTransfer;
					
					C.Ctl.Upload.sendFiles(dt.files);
	
					$('#far-upload-alert').hide();
				}
			}

			$(this.get('element'))
				.bind('dragenter', ignoreDrag)
				.bind('dragover', ignoreDrag)
				.bind('drop', drop);

			this.welcome_message = $('#far-upload-status').html();
			$('#far-upload-alert').hide();

			/*
			$('#file-input').change(function () {
				
				C.Ctl.Upload.sendFiles(
					$('#file-input')[0].files
				);

			});
			*/

		},
		resetUpload: function () {
			$("#far-upload-status").html('Drop JAR or <a href="#">Browse</a>');
			C.Ctl.Flows.load();
		},
		reset: function () {
			$('#far-upload-status').html(this.welcome_message);
		},
		cancel: function () {
			App.router.transitionTo('home');
		}
	});

});