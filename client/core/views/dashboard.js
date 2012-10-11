//
// Dashboard View
//

define([
	], function () {
	
	return Em.View.extend({
		templateName: 'dashboard',
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

			$('#upload-dropzone')
				.bind('dragenter', ignoreDrag)
				.bind('dragover', ignoreDrag)
				.bind('drop', drop);

			this.welcome_message = $('#far-upload-status').html();
			$('#far-upload-alert').hide();

			$('#file-input').change(function () {
				
				C.Ctl.Upload.sendFiles(
					$('#file-input')[0].files
				);

			});

		},
		resetUpload: function () {
			$("#far-upload-status").html('<input type="file" id="file-input" multiple />');
			C.Ctl.Flows.load();
		}
	});

});