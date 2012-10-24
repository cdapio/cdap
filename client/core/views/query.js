
define([
	], function () {
	
	return Em.View.extend({
		templateName: 'query',
		currentBinding: 'controller.current',
		requestMethod: function () {
			if (this.get('current') && this.get('current').serviceName === 'twitter') {
				return 'getTopTags';
			} else {
				return 'readactivity';
			}
		}.property('current'),
		requestParams: function () {

			if (this.get('current') && this.get("current").serviceName === 'feedreader') {
				return 'limit=10&clusterid=1';
			} else {
				return '';
			}

		}.property('current'),
		responseBody: null,
		responseCode: null,
		submit: function (event) {

			var self = this;
			C.get('gateway', {
				method: 'query',
				params: {
					service: this.current.serviceName,
					method: this.get('requestMethod'),
					query: this.get('requestParams')
				}
			}, function (error, response) {

				if (error) {
					self.set('responseCode', error.statusCode);
					self.set('responseBody', error.response || '[ No Content ]');
				} else {
					self.set('responseCode', response.statusCode);
					var pretty;
					try {
						pretty = JSON.stringify(JSON.parse(response.params.response), undefined, 2);
					} catch (e) {
						pretty = response.params.response;
					}
					self.set('responseBody', pretty || '[ No Content ]');
				}

			});

		},

		exec: function (event) {
			
			var control = $(event.target);
			if (event.target.tagName === "SPAN") {
				control = control.parent();
			}

			var id = control.attr('flow-id');
			var app = control.attr('flow-app');
			var action = control.attr('flow-action');

			if (action.toLowerCase() in C.Ctl.Query) {
				C.Ctl.Query[action.toLowerCase()](app, id, -1);
			}
		},

		promote: function (event) {

			var id = $(event.target).attr('flow-id');
			var app = $(event.target).attr('flow-app');

			C.Vw.Modal.show(
				"Push to Cloud",
				"Are you sure you would like to push this query to the cloud?",
				$.proxy(function () {

					var app = this.app;
					var id = this.id;

					C.interstitial.loading('Pushing to Cloud...', 'abc');
					window.scrollTo(0,0);
					C.get('far', {
						method: 'promote',
						params: [app, id, -1]
					}, function (error, response) {
						
						C.interstitial.hide('abc');

					});


				}, {
					id: id,
					app: app
				}));

		},
		"delete": function (event) {

			var id = $(event.target).attr('flowId');
			var app = $(event.target).attr('applicationId');
			var state = $(event.target).attr('state');

			if (state !== 'STOPPED' &&
				state !== 'DEPLOYED') {
				C.Vw.Modal.show(
					"Cannot Delete",
					"The query is currently running. Please stop it first."
				);
			} else {
				C.Vw.Modal.show(
					"Delete Query",
					"You are about to remove a Query, which is irreversible. You can upload this query again if you'd like. Do you want to proceed?",
					$.proxy(this.confirmed, {
						id: id,
						app: app,
						state: state
					}));
			}
		},
		confirmed: function () {

			C.get('far', {
				method: 'remove',
				params: [this.app, this.id, -1]
			}, function (error, response) {

				if (error) {
					C.Vw.Informer.show(error.message, 'alert-error');
				} else {
					window.history.go(-1);
				}

			});
		}

	});
});