
define([], function () {

	return Em.View.extend({

		updateData: function () {

			var metrics = this.get('metrics');

			for (var i = 0; i < metrics.length; i ++) {
				var metric = metrics[i];

				if (this.get('model') && this.get('model').metricData) {

					metric = metric.replace(/\./g, '');
					var data = this.get('model').metricData[metric];

					if (data && data.length) {
						if (!this.get('sparkline')) {

							this.get('container').html('');
							this.get('container').css({margin: ''});
							this.get('label').show();

							var widget = d3.select(this.get('container')[0]);
							var sparkline = C.util.sparkline(widget, [],
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

			var id = this.get('entity-id');
			var type = this.get('entity-type');
			var ctl = this.get('controller');

			if (type && ctl.get('types.' + type)) {

				var content = ctl.get('types.' + type).get('content');
				for (var i = 0; i < content.length; i ++) {
					if (content[i].get('id') === id) {
						this.set('model', content[i]);
						break;
					}
				}

			} else {

				this.set('model', ctl.get('current'));

			}

			return this.get('model');

		},
		setModelMetrics: function () {

			if (!this.get('model')) {
				return;
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

				while (i--) {
					metrics[i] = metrics[i].replace(/\{id\}/g, entityId);
				}

				// Hax for flowlet Queue
				if (this.get('streamId') && C.Ctl.Flow.current) {

					var flowId = C.Ctl.Flow.current.id;
					var flowletId = C.router.applicationController.view.get('flowlet-detail').controller.current.id;

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


		}.observes('model'),
		__loadingData: function (begin) {

			return;
			
		}.observes('model.__loadingData'),
		__titles: {
			'processed.count': 'Tuples per Second',
			'tuples.read.count': 'Reads per Second',
			'emitted.count': 'Tuples Emitted',
			'dataops.count': 'Data Ops per Second',
			'busyness': 'Busyness',
			'flowlet.failure.count': 'Failures per Second',
			'storage.trend': 'Storage Trend'
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
				value = C.util.bytes(value);
				return value[0] + (this.get('listMode') ? '' : '<br /><span>' + value[1] + '</span>');
			} else {
				return C.util.number(value) + (this.get('listMode') ? '' : '<br /><span>TPS</span>');
			}
		},
		didInsertElement: function () {

			var entityId = this.get('entity-id');
			var entityType = this.get('entity-type');
			var metricNames = this.get('metrics') || '';
			var color = this.get('color');

			var metrics = metricNames.split(',');
			if (metrics.length === 1 && metrics[0] === ''){
				metrics = [];
			}

			this.set('metrics', metrics);

			var height = parseInt(this.get('height'), 10),
				width = parseInt(this.get('width'), 10), label, container;

			height = height || 70;
			width = width || $(this.get('element')).parent().outerWidth();

			if (entityType === "Flowlet") {

				$(this.get('element')).addClass('white');
				label = $('<div class="sparkline-flowlet-value" />').appendTo(this.get('element'));
				$(this.get('element')).append('<div class="sparkline-flowlet-title">' + this.__getTitle() + '</div>');
				
				container = $('<div class="sparkline-flowlet-container" />').appendTo(this.get('element'));
				width = width - 44;

			} else if (this.get('listMode') || entityType) {

				this.set('listMode', true);

				$(this.get('element')).addClass(color || 'blue');
				label = $('<div class="sparkline-list-value" />').appendTo(this.get('element'));
				container = $('<div class="sparkline-list-container"><div class="sparkline-list-container-empty">&nbsp;</div></div>').appendTo(this.get('element'));
				height = 34;
				width = width - 70;

			} else {

				label = $('<div class="sparkline-box-value" />').appendTo(this.get('element'));
				container = $('<div class="sparkline-box-container" />');
				
				if (this.get('mode') === "dash") {
					container.addClass('sparkline-box-container-white');
					$(this.get('element')).append('<div class="sparkline-box-title" style="padding-left:0;background-color:#fff;">' + this.__getTitle() + '</div>');
					container.appendTo(this.get('element'));
					container = $('<div />').appendTo(container);

					width = width - 48;

				} else {
					container.addClass('sparkline-box-container');
					$(this.get('element')).append('<div class="sparkline-box-title">' + this.__getTitle() + '</div>');
					container.appendTo(this.get('element'));
					container = $('<div style="height: 69px;" />').appendTo(container);

					width = width - 68;

				}
			}

			if (entityType === 'Stream') {
				this.set('listMode', false);
			}

			this.set('width', width);
			this.set('height', height);

			this.set('label', label);
			this.set('container', container);

			this.get('container').css({marginRight: '0'});
			this.get('label').hide();

			if (this.get('unit') === 'percent' && entityType !== 'Flowlet' && !this.get('listMode')) {
				this.get('label').css({
					paddingTop: '38px'
				});
			}

			if (!metrics.length) {
				C.debug('NO METRIC FOR sparkline', this);

			} else {
				if (this.get('listMode')) {
					this.addObserver('controller.types.' + entityType + '.content', this, this.updateModel);
				} else {
					this.addObserver('controller.current', this, this.updateModel);
				}

				// Now that we've set the listener, switch 'singular' charts back to listmode
				if (this.get("mode") === 'singular') {
					this.set('listMode', true);
				}

				this.updateModel();
			}
		}
	});
});
