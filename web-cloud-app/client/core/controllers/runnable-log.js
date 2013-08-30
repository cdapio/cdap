/*
 * Runnable Log Controller
 */

define([], function () {

	var SCROLL_BUFFER = 5;

	var READ_BUFFER_HEIGHT = 126;

	var Controller = Em.Controller.extend({

		load: function () {
			var self = this;
			this.set('fromOffset', -1);
			this.set('maxSize', 50);
			this.set('initialOffset', null);
			this.set('autoScroll', true);
			var beforeHTML = $('#logView').html(),
			afterHTML;

			function resize () {
				$('#logView').css({height: ($(window).height() - 240) + 'px'});
			}

			/**
			 * Monitors changes in log box.
			 */
			function logBoxChange () {
				var logViewData = $('#logView').html();
				if (!logViewData) {
					$('#logView').html('[No Log Messages]');
					return;
				}
				afterHTML = $('#logView').html();
				if (beforeHTML !== afterHTML) {
					beforeHTML = afterHTML;
					if (self.get('autoScroll')) {
						self.scrollLogDown();
					}
				}
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

				self.HTTP.get('logs', 'getLogNext', app, id , self.get('entityType'),
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
							response = JSON.stringify(response.error);
						}


						if (response.result.length) {
							
							for (var i = 0; i < response.result.length; i ++) {
								response.result[i].logLine = '<code>' + response.result[i].logLine + '</code>';
								
								// Determines offset of last line shown in log view.
								fromOffset = (response.result[i].offset > fromOffset ?
									response.result[i].offset : fromOffset);
								
								if (!self.get('initialOffset')) {
									self.set('initialOffset', response.result[i].offset);
								}
							
							}
							response = response.result.map(function (entry) {
								return entry.logLine;
							}).join('');

						}
						$('#logView').append(response);

						// New data fetched, reset scroll position.
						logBoxChange();
					}
				);

				self.set('fromOffset', fromOffset);
				self.set('maxSize', maxSize);

			}

			setTimeout(function () {
				logInterval();
				$('#logView').bind('scroll', self.setAutoScroll.bind(self));
			}, C.EMBEDDABLE_DELAY);

			this.interval = setInterval(logInterval, C.POLLING_INTERVAL);

		},

		interval: null,
		unload: function () {

			clearInterval(this.interval);

		},

		/**
		 * Fetches logs upon scrolling to the top of the logbox div and resets scrolling for smooth
		 * transitioning.
		 */
		logUp: function () {
			// Marker for first line.
			var firstLine = $('#logView code:first');

			var self = this;
			var app = this.get('model').app;
			var id = this.get('model').name;
			var maxSize = this.get('maxSize');
			var initialOffset = this.get('initialOffset');

			self.HTTP.get('logs', 'getLogPrev', app, id , self.get('entityType'),
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
							response = JSON.stringify(response.error);
						}


						if (response.result.length) {
							for (var i = 0; i < response.result.length; i ++) {
								response.result[i].logLine = '<code>' + response.result[i].logLine + '</code>';

								// Reset offset if the current line is older than inital line.
								if (response.result[i].offset < initialOffset) {
									initialOffset =  response.result[i].offset;
								}

							}

							response = response.result.map(function (entry) {
								return entry.logLine;
							}).join('');

						}
						self.set('initialOffset', initialOffset);

						// Add response to beginning of log view and leave space for readability.
						$('#logView').prepend(response);
						$('#logView').scrollTop(firstLine.offset().top - READ_BUFFER_HEIGHT);
					}
				);
		},

		/**
		 * Determines whether user has scrolled to the bottom of the div and enables/disables auto
		 * scrolling of logs.
		 * @param {Object} event passed through event handler.
		 */
		setAutoScroll: function (event) {
			var elem = $(event.currentTarget);
			var position = elem.scrollTop();
			if (position < 1) {
				this.logUp();
			}
			if (elem[0].scrollHeight - position - elem.outerHeight() < SCROLL_BUFFER) {
				this.set('autoScroll', true);
			} else {
				this.set('autoScroll', false);
			}
		},

		/**
		 * Scrolls log view down to the very bottom.
		 */
		scrollLogDown: function() {
			$('#logView').scrollTop($('#logView')[0].scrollHeight - $('#logView').height());
		}

	});

	Controller.reopenClass({
		type: 'RunnableLog',
		kind: 'Controller'
	});

	return Controller;

});