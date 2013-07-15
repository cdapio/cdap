/*
 * Runnable Log Controller
 */

define([], function () {

	var ENTITY_MAP = {
		'FLOW': 1
	};

	var Controller = Em.Controller.extend({


		load: function () {
			this.set('fromOffset', -1);
			this.set('maxSize', 100);
			this.set('initialOffset', null);
			var self = this;

			function resize () {
				$('#logView').css({height: ($(window).height() - 240) + 'px'});
			}

			resize();

			var goneOver = false;
			var app = this.get('model').app;
			var id = this.get('model').name;
			var fromOffset = this.get('fromOffset');
			var maxSize = this.get('maxSize');

			function logInterval () {

				if (C.currentPath !== self.get('expectedPath')) {
					clearInterval(self.interval);
					return;
				}

				resize();

				self.HTTP.get('logs', 'getLogNext', app, id , ENTITY_MAP['FLOW'],
					{
						fromOffset: fromOffset,
						maxSize: maxSize,
						filter: ''
					},
					function (response) {

						if (C.currentPath !== self.get('expectedPath')) {
							clearInterval(self.interval);
							return;
						}

						if(response.error) {
							response = JSON.stringify(error);
						}


						if (response.result.length) {
							
							for (var i = 0; i < response.result.length; i ++) {
								response.result[i].logLine = '<code>' + response.result[i].logLine + '</code>';
								fromOffset = response.result[i].offset > fromOffset ? response.result[i].offset : fromOffset;
								
								if (!self.get('initialOffset')) {
									self.set('initialOffset', response.result[i].offset);
								}
							
							}
							response = response.result.map(function (entry) {
								return entry.logLine;
							}).join('');

						}
						$('#logView').append(response);
					var textarea = $('#logView');

					setTimeout(function () {

						// Content exceeds height
						if (textarea[0].scrollHeight > textarea.height()) {

							if (!goneOver) {
								textarea.scrollTop(textarea[0].scrollHeight);
								goneOver = true;
							}

							// Scrolled off the bottom
							if (textarea[0].scrollTop + textarea.height() > textarea[0].scrollHeight) {
								textarea.scrollTop(textarea[0].scrollHeight);
							}

						}

					}, C.EMBEDDABLE_DELAY);
					}
				);

				self.set('fromOffset', fromOffset);
				self.set('maxSize', maxSize);

			}

			setTimeout(function () {
				logInterval();
			}, C.EMBEDDABLE_DELAY);

			this.interval = setInterval(logInterval, C.POLLING_INTERVAL);

		},

		interval: null,
		unload: function () {

			clearInterval(this.interval);

		},

		logUp: function () {
			var self = this;
			var app = this.get('model').app;
			var id = this.get('model').name;
			var maxSize = this.get('maxSize');
			var initialOffset = this.get('initialOffset');

			self.HTTP.get('logs', 'getLogPrev', app, id , ENTITY_MAP['FLOW'],
					{
						fromOffset: initialOffset,
						maxSize: maxSize,
						filter: ''
					},
					function (response) {

						if (C.currentPath !== self.get('expectedPath')) {
							clearInterval(self.interval);
							return;
						}

						if(response.error) {
							response = JSON.stringify(error);
						}


						if (response.result.length) {
							for (var i = 0; i < response.result.length; i ++) {
								response.result[i].logLine = '<code>' + response.result[i].logLine + '</code>';

								if (response.result[i].offset < initialOffset) {
									initialOffset =  response.result[i].offset;
								}

							}

							response = response.result.map(function (entry) {
								return entry.logLine;
							}).join('');

						}
						self.set('initialOffset', initialOffset);

						$('#logView').prepend(response);

					}
				);
		},

		logDown: function() {
			$("#logView").animate({
  		  
  		  scrollTop:$("#logView")[0].scrollHeight - $("#logView").height()
			
			}, 200);
		}

	});

	Controller.reopenClass({
		type: 'RunnableLog',
		kind: 'Controller'
	});

	return Controller;

});