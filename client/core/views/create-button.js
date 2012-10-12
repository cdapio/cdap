//
// Create Button View
//

define([], function () {
	
	return Em.View.extend({
		tagName: 'button',
		classNames: ['btn', 'create-btn', 'pull-right'],
		template: Em.Handlebars.compile('+'),
		click: function () {

			var view = C.Vw.Create.create({
				entityType: this.get('entityType')
			});
			view.append();

		},
		didInsertElement: function () {

			if (this.get('entityType') === 'Flow') {
				$(this.get('element')).hide();
			}

		}
	});
});