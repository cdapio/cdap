/*
 * Chart Embeddable
 */

define([], function () {

	var Embeddable = Em.View.extend({

		updateData: function (redraw) {
			var self = this;
			var count = 0;
			var buffer = (C.POLLING_INTERVAL / 1000);
			var metrics = this.get('metrics');
      var curModel = this.get('model') || this.get('content');

			clearInterval(self.interval);

			for (var i = 0; i < metrics.length; i ++) {
				var metric = metrics[i];

				if (curModel && curModel.timeseries &&
          curModel.timeseries[C.Util.enc(metric)]) {

					var data = curModel.timeseries[C.Util.enc(metric)].value;

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
						self.get('label').html(self.__formatLabel(data[data.length - buffer]));

						self.interval = setInterval(function () {
							if (++count < buffer) {
								var labelValue = data[data.length - buffer + count];
								self.get('label').html(self.__formatLabel(labelValue));
							}
						}, 1000);

					}
				}
			}

		},

		updateModel: function () {

			var id = this.get('entityId');
			var type = this.get('entityType');
			var ctl = this.get('controller');
      var curModel = this.get('model') || this.get('content');

			if (type && ctl.get('elements.' + type)) {

				var content = ctl.get('elements.' + type).get('content');
				for (var i = 0; i < content.length; i ++) {
					if (content[i].get('id') === id) {
						this.set('model', content[i]);
						break;
					}
				}

			} else {

				this.set('model', ctl.get('model') || curModel);

			}

			// pleaseObserve indicates which metric to observe on the model.
			if (this.get('model.pleaseObserve')) {

				var metric = this.get('model.pleaseObserve');
				this.set('metrics', [ metric ]);

				this.addObserver('model.timeseries.' + C.Util.enc(metric) + '.value', this, this.updateData);

			} else {

				if (typeof this.get('model').trackMetric === 'function') {

					var metrics = this.get('metrics');
					var i = metrics.length;

					while (i--) {

						metrics[i] = this.get('model').trackMetric(metrics[i], 'timeseries') || metrics[i];
						this.addObserver('model.timeseries.' + C.Util.enc(metrics[i]) + '.value', this, this.updateData);

					}

				}

			}

		},
		__loadingData: function (begin) {

			return;

		}.observes('model.__loadingData'),
		__getTitle: function () {

			if (this.get('title')) {
				return this.get('title');
			}

			var title = [];
			var metrics = this.get('metrics');
			var i = metrics.length;
			while (i--) {
				title.push(metrics[i]);
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

				return value[0] + (this.get('listMode') ? value[1] : '<br /><span>' + value[1] + 'ps</span>');
			} else {

				var unit = this.get('unit') ? this.get('unit') : 'EPS';

				value = C.Util.number(value);
				return value[0] + value[1] + (this.get('listMode') ? '' : '<br /><span>' + unit + '</span>');
			}
		},
		fillContainer: function (rerender) {

			var width = this.get('width') || $(this.get('container')).outerWidth();
			var height = this.get('height') || $(this.get('container')).outerHeight();

			if (rerender) {
				width = $(this.get('container')).outerWidth();
				width *= 1.1;
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

			if (this.get("grid")) {

				$(this.get('element')).addClass('white');
				label = $('<div class="sparkline-flowlet-value" />').appendTo(this.get('element'));
				$(this.get('element')).append('<div class="sparkline-flowlet-title">' + this.__getTitle() + '</div>');

				container = $('<div class="sparkline-flowlet-container" />').appendTo(this.get('element'));
				this.set('overlapX', 23);

			} else if (this.get('listMode') || entityType) {

				this.set('listMode', true);

				$(this.get('element')).addClass(color || 'blue');
				label = $('<div class="sparkline-list-value" />').appendTo(this.get('element'));
				container = $('<div class="sparkline-list-container"><div class="sparkline-list-container-empty">&nbsp;</div></div>').appendTo(this.get('element'));
				this.set('overlapX', 52);
				this.set('height', 38);

			} else {

				label = $('<div class="sparkline-box-value" />').appendTo(this.get('element'));
				container = $('<div class="sparkline-box-container" />');

				container.addClass('sparkline-box-container');
				$(this.get('element')).append('<div class="sparkline-box-title">' + this.__getTitle() + '</div>');
				container.appendTo(this.get('element'));
				this.set('overlapX', 54);
				this.set('height', 70);

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

			if (this.get('listMode')) {
				this.addObserver('controller.types.' + entityType + '.content', this, this.updateModel);
			} else {
				this.addObserver('controller.model', this, this.updateModel);
			}

			// Now that we've set the listener, switch 'singular' charts back to listmode
			if (this.get("mode") === 'singular') {
				this.set('listMode', true);
				this.set('overlapX', 50);
			}

			this.updateModel();

			this.fillContainer();

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
