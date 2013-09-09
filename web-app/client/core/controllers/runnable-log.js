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

				self.HTTP.rest('apps', app, self.get('entityType'), id, 'logs', 'next',
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


						if (response.length) {
							
							for (var i = 0; i < response.length; i ++) {
								response[i].log = '<code>' + response[i].log + '</code>';
								
								// Determines offset of last line shown in log view.
								fromOffset = (response[i].offset > fromOffset ?
									response[i].offset : fromOffset);
								
								if (!self.get('initialOffset')) {
									self.set('initialOffset', response[i].offset);
								}
							
							}
							response = response.map(function (entry) {
								return entry.log;
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
				$('#logView').on('DOMMouseScroll mousewheel', self.setAutoScroll.bind(self));
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

			// If logs are currently being fetched, don't do anything.
			if (this.get('logUpPending')) {
				return;
			}

			$('#warning').html('<div>Fetching logs...</div>').show();
			this.set('logUpPending', true);

			// Marker for first line.
			var firstLine = $('#logView code:first');

			var self = this;
			var app = this.get('model').app;
			var id = this.get('model').name;
			var maxSize = this.get('maxSize');
			var initialOffset = this.get('initialOffset');

			self.HTTP.rest('apps', app, self.get('entityType'), id, 'logs', 'prev',
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


						if (response.length) {
							for (var i = 0; i < response.length; i ++) {
								response[i].log = '<code>' + response[i].log + '</code>';

								// Reset offset if the current line is older than inital line.
								if (response[i].offset < initialOffset) {
									initialOffset =  response[i].offset;
								}

							}

							response = response.map(function (entry) {
								return entry.log;
							}).join('');

						}
						self.set('initialOffset', initialOffset);

						// Add response to beginning of log view and leave space for readability.
						$('#logView').prepend(response);
						$('#logView').scrollTop(firstLine.offset().top - READ_BUFFER_HEIGHT);
						$('#warning').html('').hide();
						self.set('logUpPending', false);
					}
				);
		},

		/**
		 * Checks where a div is overflowing (if content is greater than div height).
		 * @param  {DOM Element} el Element to check.
		 * @return {Boolean} Whether div is overflowing.
		 */
		isOverflowing: function (el) {
			return el.clientHeight < el.scrollHeight;
		},

		/**
		 * Determines whether user has scrolled to the bottom of the div and enables/disables auto
		 * scrolling of logs. If user has scrolled up in a small div or scrolled to top, it fetches logs
		 * from the past.
		 * @param {Object} event passed through event handler.
		 */
		setAutoScroll: function (event) {
			var elem = $(event.currentTarget);

			// Log box is larger than logs inside of it, no scroll bars appears, mouse wheel up causes 
			// logs to be fetched.
			if (!this.isOverflowing(elem[0]) && event.originalEvent.wheelDelta >= 0) {
				this.logUp();
				this.set('autoScroll', false);
				return;
			}

			// Log box is overflowing, check if scroll bar position is at the top, if so get logs.
			var position = elem.scrollTop();
			if (position < 1 && event.originalEvent.wheelDelta >= 0) {
				this.logUp();
			}

			// If scroll bar is at the bottom, set auto scrolling to true. This is by default false if the
			// log div is not overflowing.
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