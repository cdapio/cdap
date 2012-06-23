
define([
	'lib/text!../../templates/flowlet.html',
	'lib/text!../../templates/input-stream.html'
	], function (Flowlet, Input) {

		Ember.TEMPLATES['flowlet'] = Em.Handlebars.compile(Flowlet);
		Ember.TEMPLATES['input-stream'] = Em.Handlebars.compile(Input);
	
	return Em.View.create({
		templateName: function () {
			if (!this.get('current')) {
				return 'input-stream';
			} else if (this.get('current').id === 'input-stream') {
				return 'input-stream';
			} else {
				return 'flowlet';
			}
		}.property('current'),
		currentBinding: 'App.Controllers.Flow.flowlet',
		classNames: 'flowletviz',
		location: [],
		show: function (x, y) {

			this.location = [x, y];
			this.rerender();
			
		},
		didInsertElement: function () {

			var el = $(this.get('element'));
			var x = this.location[0];
			var y = this.location[1];

			if (typeof x === 'number') {
				el.hide();
				el.fadeIn(250);

				el.css({
					top: y + 'px',
					left: x + 'px'
				});
			}

		},
		hide: function () {
			var el = $(this.get('element'));
			el.hide();
		},
		payload: null,
		PayloadView: Ember.TextField.extend({
			valueBinding: 'App.Views.Flowlet.payload',
			insertNewline: function() {
				var value = this.get('value');
				if (value) {
					App.Views.Flowlet.inject();
				}
			}
		}),
		inject: function () {

			var payload = this.get('payload');
			var flow = App.Views.Flow.current.get('meta').name;
			var stream = '';
			if (App.Views.Flow.current.get('flowStreams')) {
				stream = App.Views.Flow.current.get('flowStreams')[0].name;
			}

			this.set('payload', '');

			App.socket.request('gateway', {
				method: 'POST',
				params: {
					name: flow,
					stream: stream,
					payload: payload
				}
			}, function (response) {
				console.log(response);
			});
		}
	}).append();

});