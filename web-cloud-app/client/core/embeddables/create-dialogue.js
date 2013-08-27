/*
 * Create Dialogue Embeddable
 */

define([
	'core/lib/text!core/partials/upload.html'
	], function (App) {

		Em.TEMPLATES['create-application'] = Em.Handlebars.compile(App);

	var Embeddable = Em.View.extend({
		templateName: function () {
			return 'create-' + this.get('entityType').toLowerCase();
		}.property('entityType'),
		classNames: ['modal', 'hide', 'fade', 'narrow'],
		entityType: null,
		form: {type: null},
		datasetTypes: ['BASIC', 'TIME_SERIES', 'COUNTER', 'CSV'],
		availableApps: function () {

		},
		__idWasModified: false,
		__idHelper: function () {
			var id = this.form.name.toLowerCase().replace(/'/g, '').replace(/\W/g, '-');
			if (!this.__idWasModified) {
				this.set('form.id', id);
			}
		}.observes('form.name'),
		idChanged: function () {
			this.get('parentView').__idWasModified = true;
		},
		init: function () {
			this._super();
			// C.Ctl.Upload.set('warningMessage', '');
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
		submit: function () {

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

	Embeddable.reopenClass({
		type: 'Create',
		kind: 'Embeddable'
	});

	return Embeddable;

});