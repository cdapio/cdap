
define([
	], function () {
	
	return Em.View.extend({
		template: Em.Handlebars.compile('{{controller.message}}'),
		classNames: ['drop-zone'],
		init: function () {
			this._super();
			this.set('controller', C.Ctl.Upload);
		},
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

			var self = this;
			var file = $(this.get('element')).parent().parent().parent().find('input[type=file]');

			file.change(function () {
				
				C.Ctl.Upload.sendFiles(
					file[0].files
				);

			});
			
		}
	});

});