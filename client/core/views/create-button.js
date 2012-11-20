//
// Create Button View
//

define([], function () {
	
	return Em.View.extend({
		tagName: 'button',
		classNames: ['btn', 'create-btn', 'pull-right'],
		template: Em.Handlebars.compile('Create'),
		entityType: 'Application',
		click: function () {

			var view = C.Vw.Create.create({
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
});