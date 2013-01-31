
define([
	], function () {
	
	return Em.View.extend({
		classNames: ['drop-zone'],
		init: function () {
			this._super();
			this.set('controller', C.Ctl.Upload);

			this.set('template', Em.Handlebars.compile('Drop a JAR to ' + 
				(this.get('uploadType') || 'Deploy')));

		},
		didInsertElement: function () {

			function ignoreDrag(e) {
				e.originalEvent.stopPropagation();
				e.originalEvent.preventDefault();
			}

			var self = this;
			var element = $(this.get('element'));

			function drop (e) {
				ignoreDrag(e);

				element.removeClass('drop-zone-hover');
				element.addClass('drop-zone-loading');
				element.html('');

				if (!C.Ctl.Upload.processing) {
					var dt = e.originalEvent.dataTransfer;
					C.Ctl.Upload.sendFiles(dt.files, self.get('entityType'));
					$('#far-upload-alert').hide();
				}
			}

			$(this.get('element'))
				.bind('dragenter', function (e) {

					element.addClass('drop-zone-hover');
					ignoreDrag(e);

				})
				.bind('dragover', ignoreDrag)
				.bind('dragleave', function (e) {

					element.removeClass('drop-zone-hover');

				})
				.bind('drop', drop);

			var file = $(this.get('element')).parent().parent().parent().find('input[type=file]');

			file.change(function () {
				
				C.Ctl.Upload.sendFiles(file[0].files, self.get('entityType'));

			});
			
		}
	});

});