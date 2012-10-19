//
// Create Button View
//

define([], function () {
	
	return Em.View.extend({
		tagName: 'button',
		classNames: ['btn', 'create-btn', 'pull-right'],
		classNameBindings: ['buttonColor'],
		buttonColor: function () {

			if (this.get('entityType') === 'Application') {
				return '';
			}
			return 'btn-success';

		}.property('entityType'),
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