
define([
	'lib/text!../partials/flows.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		upload: function (event) {
			App.router.transitionTo('upload');
		},
		didInsertElement: function () {

			var data = [3, 6, 2, 7, 5, 2, 1, 3, 8, 9, 2, 5, 7],
				w = 240,
				h = 88,
				margin = 6,
				widget = d3.select('#info').append('div').attr('class', 'info-box sparkline-box');

			App.util.sparkline(widget, data, w, h, margin);
			$('#info').append('<div class="info-box sparkline-value"><strong>50</strong><br />tuples/sec</div>');

			var data = [10, 11, 12, 20, 10, 30, 20, 25, 35, 45],
				w = 240,
				h = 88,
				margin = 12,
				widget = d3.select('#info').append('div').attr('class', 'info-box sparkline-box');

			App.util.sparkline(widget, data, w, h, margin);
			$('#info').append('<div class="info-box sparkline-value"><strong>50%</strong><br />busyness</div>');

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

			$('#upload-dropzone')
				.bind('dragenter', ignoreDrag)
				.bind('dragover', ignoreDrag)
				.bind('drop', drop);

			this.welcome_message = $('#far-upload-status').html();
			$('#far-upload-alert').hide();

		},
		resetUpload: function () {
			$("#far-upload-status").html('Drop a JAR or <a href="#">Browse</a>');

			App.Controllers.Flows.load();

		}
	});

});