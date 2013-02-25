//
// App Status View
//

define([
	], function () {
	
	return Em.View.extend({
		templateName: 'application',
		currentBinding: 'controller.current',
		"delete": function () {
			
			C.Vw.Modal.show(
				"Delete Application",
				"Are you sure you would like to delete this Application? This action is not reversible.",
				$.proxy(this.confirmed, this));

		},
		confirmed: function (event) {

			var flow = this.current;
			
			C.get('metadata', {
				method: 'deleteApplication',
				params: ['Application', {
					id: this.current.id
				}]
			}, function (error, response) {

				if (error) {
					C.Vw.Informer.show(error.message, 'alert-error');
				} else {
					window.history.go(-1);
				}

			});
		},
		promote: function () {

			var viz = C.router.applicationController.view.get('push-detail');
			viz.show(this.current);

			/*

			var flow = this.current;

			C.Vw.Modal.show(
				"Push to Cloud",
				"Are you sure you would like to push this flow to the cloud?",
				$.proxy(function () {

					var flow = this;

					C.interstitial.loading('Pushing to Cloud...', 'abc');
					window.scrollTo(0,0);
					
					C.get('far', {
						method: 'promote',
						params: [flow.applicationId, flow.id, flow.version]
					}, function (error, response) {
						
						C.interstitial.hide('abc');

					});


				}, flow));


			*/


		},
		startAllFlows: function (event) {

			var container = $(event.target).parent();

			container.find('button').hide();
			container.find('img').show();

			C.Ctl.Application.startAllFlows(function () {
				container.find('button').show();
				container.find('img').hide();
			});

		},
		stopAllFlows: function (event) {

			var container = $(event.target).parent();
			
			container.find('button').hide();
			container.find('img').show();

			C.Ctl.Application.stopAllFlows(function () {
				container.find('button').show();
				container.find('img').hide();
			});

		},
		startAllQueries: function (event) {

			var container = $(event.target).parent();

			container.find('button').hide();
			container.find('img').show();

			C.Ctl.Application.startAllQueries(function () {
				container.find('button').show();
				container.find('img').hide();
			});

		},
		stopAllQueries: function (event) {

			var container = $(event.target).parent();

			container.find('button').hide();
			container.find('img').show();

			C.Ctl.Application.stopAllQueries(function () {
				container.find('button').show();
				container.find('img').hide();
			});

		}
	});
});