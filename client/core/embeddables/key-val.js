define([
	'lib/text!../partials/key-val.html'
	], function (Template) {

		var Embeddable = Em.View.extend({

			template: Em.Handlebars.compile(Template)

		});

		Embeddable.reopenClass({
			type: 'KeyVal',
			kind: 'Embeddable',
			Field: Ember.TextField.extend({
				focusOut: function () {

					var kind = this.get("kind");
					var index = this.get("index");

					if (!kind && !index) {
						if (this.get('value')) {

							var key = $('.config-editor-new .config-editor-key input').val();
							var value = $('.config-editor-new .config-editor-value input').val();

							if (key && value) {

								$('.config-editor-new .config-editor-key input').val('');
								$('.config-editor-new .config-editor-value input').val('');

								this.get('controller').addDone(key, value);

							}

						}
					} else {
						this.get('controller').editDone(kind, index);
					}

				},
				insertNewline: function() {

					var kind = this.get("kind");
					var index = this.get("index");

					if (!kind && !index) {

						var key = $('.config-editor-new .config-editor-key input').val();
						var value = $('.config-editor-new .config-editor-value input').val();

						if (key && value) {

							$('.config-editor-new .config-editor-key input').val('');
							$('.config-editor-new .config-editor-value input').val('');

							this.get('controller').addDone(key, value);
						}
					} else {
						this.get('controller').editDone(kind, index);
					}
				}
			})
		});

		return Embeddable;

	});