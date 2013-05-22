/*
 * Chart Embeddable
 */

define([], function () {

	var Embeddable = Em.View.extend({

		updateData: function (redraw) {

			var metrics = this.get('metrics');

			for (var i = 0; i < metrics.length; i ++) {
				var metric = metrics[i];

				if (this.get('model') && this.get('model').metricData) {

					metric = metric.replace(/\./g, '');
					var data = this.get('model').metricData[metric];

					if (data && data.length) {
						if ((typeof redraw === 'boolean' && redraw) || !this.get('sparkline')) {

							this.get('container').html('');
							this.get('container').css({margin: ''});
							this.get('label').show();

							var widget = d3.select(this.get('container')[0]);
							var sparkline = C.Util.sparkline(widget, [],
								this.get('width'), this.get('height'), this.get('unit') === 'percent');
							this.set('sparkline', sparkline);

						}

						this.get('sparkline').update(i, data);
						this.get('label').html(this.__formatLabel(data[data.length - 1]));
					}
				}
			}

		},

		updateModel: function () {

			var id = this.get('entityId');
			var type = this.get('entityType');
			var ctl = this.get('controller');

			if (type && ctl.get('elements.' + type)) {

				var content = ctl.get('elements.' + type).get('content');
				for (var i = 0; i < content.length; i ++) {
					if (content[i].get('id') === id) {
						this.set('model', content[i]);
						break;
					}
				}

			} else {

				this.set('model', ctl.get('model'));

			}

			if (typeof this.get('model').addMetricName === 'function') {

				var metrics = this.get('metrics');
				var i = metrics.length, metric;

				var entityId = this.get('model').id;

				// ** hax for StreamFlow **//
				if (this.get('model').get('streamId')) {
					entityId = this.get('model').get('streamId');
				}

				// ** hax for DatasetFlow **//
				if (this.get('model').get('datasetId')) {
					entityId = this.get('model').get('datasetId');
				}

				var account_id = C.Env.user.id;

				while (i--) {
					metrics[i] = metrics[i].replace(/\{id\}/g, entityId);
					metrics[i] = metrics[i].replace(/\{account_id\}/g, account_id);
				}

				// Hax for flowlet Queue
				if (this.get('streamId')) {

					var flow = this.get('controller').get('controllers').get('FlowStatus').get('model');
					var flowlet = this.get('controller').get('model');

					var flowId = flow.id;
					var flowletId = flowlet.id;

					i = metrics.length;
					while (i--) {
						metrics[i] = metrics[i].replace(/\{streamId\}/g, this.get('streamId'))
							.replace(/\{flowletId\}/g, flowletId)
							.replace(/\{flowId\}/g, flowId);
					}

				}

				var i = metrics.length;

				while (i--) {

					metric = metrics[i];
					this.get('model').addMetricName(metric);
					metric = metric.replace(/\./g, '');
					this.addObserver('model.metricData.' + metric, this, this.updateData);
				}

			}


		},
		__loadingData: function (begin) {

			return;

		}.observes('model.__loadingData'),
		__titles: {
			'processed.count': 'Processing Rate',
			'tuples.read.count': 'Read Rate',
			'emitted.count': 'Tuples Emitted',
			'dataops.count': 'Data Operations',
			'busyness': 'Busyness',
			'flowlet.failure.count': 'Failures Rate',
			'storage.trend': 'Storage'
		},
		__getTitle: function () {

			if (this.get('title')) {
				return this.get('title');
			}

			var title = [];
			var metrics = this.get('metrics');
			var i = metrics.length;
			while (i--) {
				title.push(this.__titles[metrics[i]] || metrics[i]);
				if (i > 0) {
					title.push(' vs. ');
				}
			}
			return title.join(' ');
		},
		__formatLabel: function (value) {
			if (this.get('unit') === 'percent' || this.get('metric') === 'busyness') {
				return Math.round(value) + '%';
			} if (this.get('unit') === 'bytes') {
				value = C.Util.bytes(value);
				return value[0] + (this.get('listMode') ? '' : '<br /><span>' + value[1] + '</span>');
			} else {
				return C.Util.number(value) + (this.get('listMode') ? '' : '<br /><span>TPS</span>');
			}
		},
		fillContainer: function (rerender) {

			var width = this.get('width') || $(this.get('container')).outerWidth();
			var height = this.get('height') || $(this.get('container')).outerHeight();

			if (rerender) {
				width = $(this.get('container')).outerWidth();
				width += width * 0.08;
			} else {
				width -= this.get('overlapX') || 0;
			}

			this.set('width', width);
			this.set('height', height);

			if (rerender) {
				this.updateData(true);
			}

		},
		didInsertElement: function () {

			var entityId = this.get('entityId');
			var entityType = this.get('entityType');
			var metricNames = this.get('metrics') || '';
			var color = this.get('color');

			var metrics = metricNames.split(',');
			if (metrics.length === 1 && metrics[0] === ''){
				metrics = [];
			}

			this.set('metrics', metrics);

			var label, container;

			if (entityType === "Flowlet") {

				$(this.get('element')).addClass('white');
				label = $('<div class="sparkline-flowlet-value" />').appendTo(this.get('element'));
				$(this.get('element')).append('<div class="sparkline-flowlet-title">' + this.__getTitle() + '</div>');

				container = $('<div class="sparkline-flowlet-container" />').appendTo(this.get('element'));
				this.set('overlapX', 40);

			} else if (this.get('listMode') || entityType) {

				this.set('listMode', true);

				$(this.get('element')).addClass(color || 'blue');
				label = $('<div class="sparkline-list-value" />').appendTo(this.get('element'));
				container = $('<div class="sparkline-list-container"><div class="sparkline-list-container-empty">&nbsp;</div></div>').appendTo(this.get('element'));
				this.set('overlapX', 50);
				this.set('height', 38);

			} else {

				label = $('<div class="sparkline-box-value" />').appendTo(this.get('element'));
				container = $('<div class="sparkline-box-container" />');

				if (this.get('mode') === "dash") {
					container.addClass('sparkline-box-container-white');
					$(this.get('element')).append('<div class="sparkline-box-title" style="padding-left:0;background-color:#fff;">' + this.__getTitle() + '</div>');
					container.appendTo(this.get('element'));
					this.set('overlapX', 36);
					this.set('height', 70);

					var self = this;
					C.addResizeHandler(this.get('entityId') + this.get('metrics').join(':'), function () {

						self.fillContainer(true);

					});

				} else {
					container.addClass('sparkline-box-container');
					$(this.get('element')).append('<div class="sparkline-box-title">' + this.__getTitle() + '</div>');
					container.appendTo(this.get('element'));
					this.set('overlapX', 64);
					this.set('height', 70);
				}
			}

			if (entityType === 'Stream') {
				this.set('listMode', false);
			}

			this.set('label', label);
			this.set('container', container);

			this.get('container').css({marginRight: '0'});
			this.get('label').hide();

			if (this.get('unit') === 'percent' && entityType !== 'Flowlet' && !this.get('listMode')) {
				this.get('label').css({
					paddingTop: '42px'
				});
			}

			this.fillContainer();

			if (!metrics.length) {

				C.debug('No metric provided for sparkline.', this);

			} else {
				if (this.get('listMode')) {
					this.addObserver('controller.types.' + entityType + '.content', this, this.updateModel);
				} else {
					this.addObserver('controller.model', this, this.updateModel);
				}

				// Now that we've set the listener, switch 'singular' charts back to listmode
				if (this.get("mode") === 'singular') {
					this.set('listMode', true);
				}

				this.updateModel();
			}

		},
		willDestroyElement: function () {

			C.removeResizeHandler(this.get('entityId') + this.get('metrics').join(':'));

		}
	});

	Embeddable.reopenClass({

		type: 'Chart',
		kind: 'Embeddable'

	});

	return Embeddable;

});
