//
// Flowlet Model
//

define([], function () {
	return Em.Object.extend({
		processed: 0,
		plural: function () {
			return this.instances === 1 ? '' : 's';
		}.property('instances'),
		doubleCount: function () {
			return 'Add ' + this.instances;
		}.property().cacheable(false),
		fitCount: function () {
			return 'No Change';
		}.property().cacheable(false),
		addInstances: function (value, done) {

			var instances = this.get('instances') + value;

			if (instances < 1 || instances > 64) {
				done('Cannot set instances. Please select an instance count > 1 and <= 64');
			} else {

				var current = this;
				var currentFlow = App.Controllers.Flow.get('current');

				var app = currentFlow.meta.app;
				var flow = currentFlow.meta.name;
				var version = currentFlow.version;

				var flowlet = current.name;

				App.interstitial.loading('Setting instances for "' + flowlet + '" flowlet to ' + instances + '.');
				App.Views.Flowlet.hide();

				App.socket.request('manager', {
					method: 'setInstances',
					params: [app, flow, version, flowlet, instances]
				}, function (error, response) {

					if (error) {
						App.Views.Informer.show(error, 'alert-error');
						App.interstitial.hide();
					} else {
						current.set('instances', instances);
						App.Views.Informer.show('Successfully set the instances for "' + flowlet + '" to ' + instances + '.', 'alert-success');
						
					}

				});

			}
		}
	});
});