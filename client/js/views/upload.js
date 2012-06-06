
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

				$('#far-upload').html('');

				var reader = new FileReader();
				reader.onload = function (evt) {

					var xhr = new XMLHttpRequest();
					if (xhr.upload) {
						xhr.upload.onprogress = function () {
							console.log(arguments);
						};
					}
					xhr.open('POST', '/upload', true);
					xhr.setRequestHeader("Content-type", "application/octet-stream");
					xhr.onload = function(e) {
						console.log('done', e);
					};
					xhr.send(evt.target.result);

				};
				reader.readAsArrayBuffer(file);

			}

			$('#far-upload')
				.bind('dragenter', ignoreDrag)
				.bind('dragover', ignoreDrag)
				.bind('drop', drop);

		},
		identifier: null,
		update: function (response) {
					console.log(response);
			var html;
			switch(response) {
				case 'sent':
					html = 'Sending data...';
				break;
				case 'verifying':
					html = 'Verifying...';
				break;
				default:
					if (response.version) {
						this.identifier = response;
						html = 'Initializing version ' + response.version + '...';
					} else {
						html = response.message + '<br /><br /><a href="#">View this Flow</a>';
					}
			}
			$('#far-upload').append('<h2>' + html + '</h2>');
		}
	});

});