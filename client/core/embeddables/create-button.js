/*
 * Create Button Embeddable
 */

define([], function () {

	var Embeddable = Em.View.extend({
		tagName: 'button',
		classNames: ['btn', 'create-btn', 'pull-right'],
		template: Em.Handlebars.compile('Add an App'),
		entityType: 'Application',
		click: function () {

			var view = C.Embed.Create.create({
				entityType: this.get('entityType')
			});
			view.append();

		},
		didInsertElement: function () {

			if (this.get('entityType') === 'Flow' ||
				this.get('entityType') === 'Query') {
				$(this.get('element')).hide();
			}

			if (this.get('entitType') === 'Application') {
				this.set('classNames', ['btn', 'create-btn', 'pull-right']);
			}

		}
	});

	Embeddable.reopenClass({
		type: 'CreateButton',
		kind: 'Embeddable'
	});

	return Embeddable;

});