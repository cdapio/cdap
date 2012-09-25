
define([
	'lib/text!../partials/flowlet.html',
	'lib/text!../partials/input-stream.html'
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
			} else if (this.get('current').isSource) {
				return 'input-stream';
			} else {
				return 'flowlet';
			}
		}.property('current'),
		currentBinding: 'controller.flowlet',
		currentFlowBinding: 'controller.current',
		classNames: 'flowletviz',
		location: [],
		modifiable: function () {

			if (this.currentFlow && this.currentFlow.currentState === 'RUNNING') {
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
			if (value) {
				value = parseInt(value, 10);
				this.confirm('Add ' + value + ' instances to ', value);
			}
		},
		confirm: function (message, value) {

			var current = this.get('current');
			var name = current.name;

			C.Vw.Modal.show(
				"Flowlet Instances",
				message + '"' + name + '" flowlet?',
				function () {
					current.addInstances(value, function () {
					
					});
				});

		},
		payload: null,
		inject: function () {

			var payload = this.get('payload');
			var flow = C.Ctl.Flow.current.get('meta').name;
			var stream = '';
			if (C.Ctl.Flow.current.get('flowStreams')) {
				stream = C.Ctl.Flow.current.get('flowStreams')[0].name;
			}

			this.set('payload', '');

			C.get('gateway', {
				method: 'POST',
				params: {
					name: flow,
					stream: stream,
					payload: payload
				}
			}, function (error, response) {

				if (error) {
					C.Vw.Flowlet.hide();
					C.Vw.Informer.show('There was a problem connecting to the gateway.', 'alert-error');
				}

			});
		}
	});

});