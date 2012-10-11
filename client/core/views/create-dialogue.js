//
// Create App, Stream, Dataset
//

define([
	'lib/text!../partials/create/create-app.html',
	'lib/text!../partials/create/create-stream.html',
	'lib/text!../partials/create/create-flow.html',
	'lib/text!../partials/create/create-dataset.html'
	], function (App, Stream, Flow, Dataset) {
	
		Em.TEMPLATES['create-application'] = Em.Handlebars.compile(App);
		Em.TEMPLATES['create-stream'] = Em.Handlebars.compile(Stream);
		Em.TEMPLATES['create-flow'] = Em.Handlebars.compile(Flow);
		Em.TEMPLATES['create-dataset'] = Em.Handlebars.compile(Dataset);

	return Em.View.extend({
		templateName: function () {
			return 'create-' + this.get('entityType').toLowerCase();
		}.property('entityType'),
		classNames: ['modal', 'hide', 'fade', 'narrow'],
		entityType: null,
		form: {type: null},
		datasetTypes: ['BASIC', 'TIME_SERIES', 'COUNTER', 'CSV'],
		__idWasModified: false,
		__idHelper: function () {
			var id = this.form.name.toLowerCase().replace(/'/g, '').replace(/\W/g, '-');
			if (!this.__idWasModified) {
				this.set('form.id', id);
			}
		}.observes('form.name'),
		idChanged: function () {
			console.log(arguments);
			this.get('parentView').__idWasModified = true;
		},
		init: function () {
			this._super();
			C.Ctl.Upload.set('warningMessage', '');
		},
		didInsertElement: function () {
			var el = $(this.get('element'));
			el.modal('show');

			setTimeout(function () {
				$($('.modal-body').find('input[type=text]')[0]).focus();
			}, 250);

		},
		warningMessage: '',
		warningMessageBinding: 'C.Ctl.Upload.warningMessage',
		showWarning: function () {
			if (this.get('warningMessage')) {
				return 'display:block;';
			}
			return 'display:none;';
		}.property('warningMessage').cacheable(false),
		confirmed: function () {

			var self = this;
			C.get('metadata', {
				method: 'create' + this.get('entityType'),
				params: [this.get('entityType'), this.form]
			}, function (error, response) {

				if (error) {
					self.set('warningMessage', error.message);
				} else {
					self.hide();
					window.location.reload();
				}

			});
		},
		hide: function () {
			var el = $(this.get('element'));
			var self = this;

			el.modal('hide');
			el.on('hidden', function () {
				self.destroyElement();
			});
		}
	});
});