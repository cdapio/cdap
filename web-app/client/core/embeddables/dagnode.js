/*
 * Flow DAG Node Embeddable
 */

define([
	'core/lib/text!core/partials/dagnode.html'
	], function (Template) {

		var Embeddable = Em.View.extend({

			template: Em.Handlebars.compile(Template),

			classNames: ['window'],

			classNameBindings: ['className'],

			init: function () {

				this._super();

				var model, id = 'unknown';
				if ((model = this.get('model'))) {
					id = 'flowlet' + model.name;
				}

				this.set('elementId', id);

			},
	
			className: function () {
				var model;
				if ((model = this.get('model'))) {
					return (model.get('type') === 'Stream' ? ' source' : '');
				}
				else {
					return 'unknown';
				}
			}.property(),

			click: function (event) {

				if (C.currentPath !== 'Flow.History') {

					if (this.get('model').get('type') === 'Stream') {
						this.get('controller').transitionToRoute('FlowStatus.Stream', this.get('model'));
					} else {
						this.get('controller').transitionToRoute('FlowStatus.Flowlet', this.get('model'));
					}

				}

			}
		});

		Embeddable.reopenClass({

			type: 'DagNode',
			kind: 'Embeddable'

		});

		return Embeddable;

	});