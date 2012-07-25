
define([
	'lib/text!../../templates/flowlet.html',
	'lib/text!../../templates/input-stream.html'
	], function (Flowlet, Input) {

	return Em.View.create({
		compile: function () {

			console.log('Compiling flowlet detail.');

			Ember.TEMPLATES.flowlet = Em.Handlebars.compile(Flowlet);
			Ember.TEMPLATES['input-stream'] = Em.Handlebars.compile(Input);

			this.append();
		},
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
		currentFlowBinding: 'App.Controllers.Flow.current',
		classNames: 'flowletviz',
		location: [],
		modifiable: function () {

			if (this.currentFlow.currentState === 'RUNNING') {
				return true;
			}
			return false;

		}.property('currentFlow.currentState'),
		reducable: function () {
			if (this.current.instances > 1) {
				return true;
			}
			return false;
		}.property('current.instances'),
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
		addOneInstance: function () {
			this.confirm('Add 1 instance to ', +1);
		},
		removeOneInstance: function () {
			this.confirm('Remove 1 instance from ', -1);
		},
		doubleInstances: function () {

			var newCount = this.get('current').instances;

			this.confirm('Double instances. Add ' + newCount + ' instance to ', newCount);
		},
		fitInstances: function () {

			var newCount = 0;

			if (newCount < 0) {
				this.confirm('Add ' + newCount + ' instances to ', newCount);
			} else if (newCount > 0) {
				this.confirm('Remove ' + newCount + ' instances from ', newCount);
			} else {
				window.alert('Instance count already fits load.');
			}

		},
		promptInstances: function () {

			var value = window.prompt('Please set the number of instances for this flowlet. At least 1.');
			value = parseInt(value, 10);
			this.confirm('Add ' + value + ' instances to ', value);

		},
		confirm: function (message, value) {

			var name = this.get('current').name;

			var c = window.confirm(message + '"' + name + '" flowlet?');
			if (c) {
				this.get('current').addInstances(value, function () {
				
				});
			}
		},
		payload: null,
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
			}, function (error, response) {

				if (error) {
					App.Views.Flow.showError('There was a problem connecting to the gateway.');
				}

			});
		}
	});

});