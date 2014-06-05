/*
 * Create Button Embeddable
 */

define([], function () {

	var Embeddable = Em.View.extend({

		tagName: 'div',
		classNames: ['create-btn', 'pull-right'],
		template: Em.Handlebars.compile('<button class="btn btn-blue" id="load-app-trigger">Load an app</button>'),
		entityType: 'Application',

		click: function () {

			// Browser does not support HTML5 File api, revert back to drag and drop.
			if (!window.File || !window.FileReader || !window.FileList || !window.Blob) {
				$('#drop-hover').one('click', function () {
					$('#drop-hover').fadeOut();
				});
				$('#drop-hover').fadeIn();
			} else {
				$('#app-upload-input').trigger('click');
			}

		},

		/**
		 * Checks if the file has uploaded.
		 * @return {Boolean} if file has uploaded.
		 */
		doneLoading: function () {
			var self = this;
			// Wait a second before executing to allow for file upload and prevent recursive checking.
			C.Util.threadSleep(1000);
			if (!$("#app-upload-input")[0].files.length) {
				self.doneLoading();
			} else {
				return true;
			}
		},

		didInsertElement: function () {
			var self = this;
			if (this.get('entityType') === 'Flow' ||
				this.get('entityType') === 'Query') {
				$(this.get('element')).hide();
			}

			if (this.get('entitType') === 'Application') {
				this.set('classNames', ['btn', 'create-btn', 'pull-right']);
			}

			$('#app-upload-input').change(function (e) {
				$('#drop-label').hide();
				$('#drop-loading').show();
				$('#drop-hover').fadeIn();
				if (self.doneLoading()) {
					var file = $('#app-upload-input')[0].files[0];
					var name = file.name;
					C.Util.Upload.sendFiles([file], name);
				}
			});

		}
	});

	Embeddable.reopenClass({
		type: 'CreateButton',
		kind: 'Embeddable'
	});

	return Embeddable;

});