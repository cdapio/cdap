
define([], function () {

		return Em.View.extend({

			updateData: function () {

				var metric = this.get('metric');
				if (metric && this.get('model') && this.get('model').metricData) {

					metric = metric.replace(/\./g, '');
					var data = this.get('model').metricData[metric];

					if (data && data.length) {
						this.get('sparkline').update(data);
						this.get('label').html(this.__formatLabel(data[data.length - 1]));
					}
				}

			},

			updateModel: function () {

				if (this.get('model')) {
					return this.get('model');
				}

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

					if (!this.get('model')) {
						this.__loadingData();
					}
				}

				if (this.get('model') && this.get('model').addMetricName) {

					var metric = this.get('metric');
					this.get('model').addMetricName(metric);

					metric = metric.replace(/\./g, '');

					this.addObserver('model.metricData.' + metric, this, this.updateData);

				}

				return this.get('model');

			},
			__loadingData: function (begin) {

				return;

				/*
				if (begin === true || this.get('model').__loadingData) {
					$(this.get('element')).addClass('sparkline-loading');
				} else {
					$(this.get('element')).removeClass('sparkline-loading');
				}
				*/
				
			}.observes('model.__loadingData'),
			__titles: {
				'processed.count': 'Processed'
			},
			__getTitle: function (metric) {
				return (this.__titles[metric] || metric);
			},
			__formatLabel: function (value) {
				return C.util.number(value) + (this.get('listMode') ? '' : '<br /><span>TPS</span>');
			},
			didInsertElement: function () {
				
				var entityId = this.get('entity-id');
				var entityType = this.get('entity-type');
				var metric = this.get('metric');
				var color = this.get('color');

				if (entityType) {
					this.set('listMode', true);
				}

				var height, width = parseInt(this.get('width'), 10) || 200,
					margin = 8, label, container;

				if (this.get('listMode')) {
					$(this.get('element')).addClass('blue');
					label = $('<div class="sparkline-list-value" />').appendTo(this.get('element'));
					container = $('<div class="sparkline-list-container" />').appendTo(this.get('element'));
					height = 36;
					width -= 32;

				} else if (this.get('flowletMode')) {

					$(this.get('element')).addClass('white');
					label = $('<div class="sparkline-flowlet-value" />').appendTo(this.get('element'));
					$(this.get('element')).append('<div class="sparkline-flowlet-title">' + this.__getTitle(metric) + '</div>');
					
					container = $('<div class="sparkline-flowlet-container" />').appendTo(this.get('element'));
					width -= 52;
					height = 70;

				} else {
					
					$(this.get('element')).append('<div class="sparkline-box-title">' + this.__getTitle(metric) + '</div>');
					label = $('<div class="sparkline-box-value" />').appendTo(this.get('element'));
					container = $('<div class="sparkline-box-container" />').appendTo(this.get('element'));
					height = 70;
				}

				this.set('label', label);

				//$(this.get('element')).addClass('sparkline-loading');

				var widget = d3.select(container[0]);
				var sparkline = C.util.sparkline(widget, [], width, height, margin);
				this.set('sparkline', sparkline);

				if (!metric) {
					C.debug('NO METRIC FOR sparkline', this);
				} else {
					metric = metric.replace(/\./g, '');
					if (this.get('listMode')) {
						this.addObserver('controller.types.' + entityType + '.content', this, this.updateModel);
					} else {
						this.addObserver('controller.current', this, this.updateModel);
					}

					this.updateModel();
				}
			}
		});
	});