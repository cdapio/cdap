
define([
	'lib/text!../../partials/list/query-list.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		didInsertElement: function () {

		},
		select: function (event) {

			var viz = this.get('query-detail');

			var id = $(event.target).attr('flowId');
			var q = this.get('controller').get('types.Query').content;
			var i = q.length;
			while (i--) {
				if (q[i].get('id') === id) {
					viz.show(q[i]);
					return;
				}
			}

		},
		stateTransition: function (event) {

			$(event.target).parent().html('<img src="/assets/img/chart-loading.gif" />');

			var id = $(event.target).attr('flowId');
			var app = $(event.target).attr('applicationId');
			var state = $(event.target).attr('state');
			var method;
			
			if (state === 'RUNNING') {
				method = 'stop';
			} else {
				method = 'start';
			}

			C.socket.request('manager', {
				method: method,
				params: [app, id, -1, 'QUERY']
			}, function (error, response) {

				setTimeout(function () {
					window.location.reload();
				}, 1000);

			});

		},
		promote: function () {

			var id = $(event.target).attr('flowId');
			var app = $(event.target).attr('applicationId');

			C.interstitial.loading('Pushing to Cloud...', 'abc');

			window.scrollTo(0,0);

			C.get('far', {
				method: 'promote',
				params: [app, id, -1]
			}, function (error, response) {
				
				C.interstitial.hide('abc');

			});

		},
		"delete": function () {

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
