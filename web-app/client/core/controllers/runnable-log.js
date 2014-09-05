/*
 * Runnable Log Controller
 */

define([], function () {

	var SCROLL_BUFFER = 5;
	var TOP_BUFFER = 800;
	var LOG_CALL_INTERVAL = 500;

	var Controller = Em.Controller.extend({

		beforeHTML: null,
		afterHTML: null,
		interval: null,

		load: function () {
			var self = this;
			this.set('fromOffset', -1);
			this.set('maxSize', 50);
			this.set('initialOffset', null);
			this.set('autoScroll', true);
			this.set('lastLogFetchTime', null);
			this.set('logType', 'ALL');
			this.set('logMetrics', Em.Object.create({
				ALL: Em.Object.create({count: 0, active: true}),
				INFO: Em.Object.create({count: 0, active: false}),
				WARN: Em.Object.create({count: 0, active: false}),
				ERROR: Em.Object.create({count: 0, active: false}),
				DEBUG: Em.Object.create({count: 0, active: false}),
				OTHER: Em.Object.create({count: 0, active: false})
			}));
			this.beforeHTML = $('#logView').html();

			self.resize();


			var goneOver = false;
			var model = this.get('model');
			var fromOffset = this.get('fromOffset');
			var maxSize = this.get('maxSize');

			/**
			 * Fetches latest logs and displays them.
			 */
			var logInterval = function () {
				if (C.currentPath !== self.get('expectedPath')) {
					clearInterval(self.interval);
					return;
				}

				self.resize();

				self.HTTP.rest(model.get('context'), 'logs', 'next',
					{
						fromOffset: fromOffset,
						maxSize: maxSize,
						filter: ''
					},
					function (response) {

						if(response.error) {
							response = JSON.stringify(response.error);
						}


						if (response.length) {

							for (var i = 0; i < response.length; i ++) {
								response[i] = self.processLogEntry(response[i]);

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

						if (response.length) {

							$('#logView').append('<div>' + response + '</div>');
						}


						// New data fetched, reset scroll position.
						self.logBoxChange();
					}
				);

				self.set('fromOffset', fromOffset);
				self.set('maxSize', maxSize);

			};

			setTimeout(function () {
				logInterval();
				$('#logView').on('DOMMouseScroll mousewheel', function (event) {
					self.setAutoScroll(event);
				});
			}, C.EMBEDDABLE_DELAY);

			this.interval = setInterval(logInterval, C.POLLING_INTERVAL);

		},

		resize: function () {
			$('#logView').css({height: ($(window).height() - 290) + 'px'});
		},

		/**
		 * Monitors changes in log box.
		 */
		logBoxChange: function () {
			var self = this;
			var logViewData = $('#logView').html();
			if (!logViewData) {
				$('#logView').html('[No Log Messages]');
				return;
			}
			this.afterHTML = $('#logView').html();
			if (this.beforeHTML !== this.afterHTML) {
				this.beforeHTML = this.afterHTML;
				if (self.get('autoScroll')) {
					self.scrollLogDown();
				}
			}
		},

		/**
		 * Gets entry type for log entry, increments counters and animates tabs wherever appropriate.
		 */
		getEntryTypeAndIncrement: function (entry) {
			var self = this;
			self.incrementMetrics('ALL', 1);
			var type = 'OTHER', types = ['INFO', 'ERROR', 'WARN', 'DEBUG'], i = types.length;
			while (i--) {
		    if (entry.indexOf('- ' + types[i]) !== -1) {
	        type = types[i];
	        break;
		    }
			}
			if (type === 'ERROR') {
		    self.animateElement('log-error-tab', 'transparent', '#d9534f');
			}

			self.incrementMetrics(type, 1);
			return type;
		},

		/**
		 * Animates element between 2 colors.
		 */
		animateElement: function (id, beforeColor, afterColor) {
			if (!$('#' + id).is(':animated')) {
				$('#' + id)
					.stop()
					.css({'background-color': beforeColor, 'color': '#77878B'})
					.animate({backgroundColor: afterColor, color: 'white'}, 500);
			}
		},

		/**
		 * Increment metrics counters for certain metric type.
		 */
		incrementMetrics: function (type, amount) {
			var count = this.get('logMetrics').get(type).get('count') + amount;
			this.get('logMetrics').get(type).set('count', count);
		},

		/**
		 * Processes a single log entry and makes adjustments for display.
		 */
		processLogEntry: function (record) {
			var self = this;
			var entryType = self.getEntryTypeAndIncrement(record.log);
			var element = $('<code data-log-type="'+entryType+'">' + record.log + '</code>');

			// Check what kind of logs user has selected to see.
			if (self.get('logType') !== 'ALL' && entryType !== self.get('logType')) {
				element.hide();
			}
			record.log = element[0].outerHTML;
			return record;
		},

		/**
		 * Show logs based on certain type, hide all other log entries.
		 */
		showLogsByType: function (type) {
			var prevPosition = $('#logView').scrollTop() / $('#logView')[0].scrollHeight;
			this.set('logType', type);
			this.setActiveTab(type);
			var self = this;
			var entries = $('#logView code');
			if (type !== 'ALL') {
				for (var i = 0; i < entries.length; i++) {
					if ($(entries[i]).attr('data-log-type') !== type) {
						$(entries[i]).hide();
					} else {
						$(entries[i]).show();
					}
				}
			} else {
				$('#logView code').show();
			}
			$('#logView').scrollTop($('#logView')[0].scrollHeight * prevPosition);
		},

		/**
		 * Sets active tab for type of logs being shown.
		 */
		setActiveTab: function (type) {
			var logMetrics = this.get('logMetrics');
			for (var metric in logMetrics) {
				if (logMetrics.hasOwnProperty(metric)) {
					logMetrics[metric].set('active', false);
				}
			}
			logMetrics.get(type).set('active', true);
		},

		/**
		 * Fetches logs upon scrolling to the top of the logbox div and resets scrolling for smooth
		 * transitioning.
		 */
		logUp: function () {
			var self = this;

			// If logs are currently being fetched, don't do anything.
			if (this.get('logUpPending')) {
				return;
			}

			if (C.currentPath !== self.get('expectedPath')) {
				clearInterval(self.interval);
				return;
			}

			this.set('logUpPending', true);

			var model = this.get('model');
			var maxSize = this.get('maxSize') || 50;
			var initialOffset = this.get('initialOffset') || -1;

			this.HTTP.rest(model.get('context'), 'logs', 'prev',
					{
						fromOffset: initialOffset,
						maxSize: maxSize,
						filter: ''
					},
					function (response) {
						self.set('lastLogFetchTime', new Date().getTime());

						if(response.error) {
							response = JSON.stringify(response.error);
						}


						if (response.length) {
							for (var i = 0; i < response.length; i ++) {
								response[i] = self.processLogEntry(response[i]);

								// Reset offset if the current line is older than inital line.
								if (response[i].offset < initialOffset) {
									initialOffset =  response[i].offset;
								}

							}

							response = response.map(function (entry) {
								return entry.log;
							}).join('');

							// Add response to beginning of log view and adjust height to match reading position
							// since prepended data changes scroll position.
							if (response.length) {
								var currentScroll = $('#logView').scrollTop();
								$('#logView').prepend('<div>' + response + '</div>');
								var responseHeight = $('#logView div:first')[0].scrollHeight;
								$('#logView').scrollTop(responseHeight + currentScroll);
							}
						}
						self.set('initialOffset', initialOffset);
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
			var self = this;
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
			var lagIntervalLapsed = (self.get('lastLogFetchTime') ?
				new Date().getTime()-self.get('lastLogFetchTime') > LOG_CALL_INTERVAL : true);
			if (position < TOP_BUFFER && event.originalEvent.wheelDelta >= 0 && lagIntervalLapsed) {
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
		},

		unload: function () {
			clearInterval(this.interval);
		}

	});

	Controller.reopenClass({
		type: 'RunnableLog',
		kind: 'Controller'
	});

	return Controller;

});