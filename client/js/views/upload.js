
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
				var dt = e.originalEvent.dataTransfer;
				var files = dt.files;

				if(files.length > 0){
					var file = dt.files[0];
					sendFile(file);
				}
			}

			function sendFile(file) {

				var formData = new FormData();
				formData.append(file.name, file);
	
				var xhr = new XMLHttpRequest();

				if (xhr.upload) {
					xhr.upload.onprogress = function () {
						console.log(arguments);
					};
				}

				xhr.open('POST', '/upload', true);
				xhr.onload = function(e) {

				};
				xhr.send(formData);

			}

			$('#far-upload')
				.bind('dragenter', ignoreDrag)
				.bind('dragover', ignoreDrag)
				.bind('drop', drop);

		}
	});

});