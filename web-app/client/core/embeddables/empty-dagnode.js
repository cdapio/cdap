/*
 * Flow DAG Node Embeddable
 */

define([], function () {

    var Embeddable = Em.View.extend({

      template: Em.Handlebars.compile('<div></div>'),
      classNames: ['window'],
      init: function () {

        this._super();

        var model, id = 'unknown';
        if ((model = this.get('model'))) {
          id = 'flowlet' + model.name;
        }

        this.set('elementId', id);

      }
    });

    Embeddable.reopenClass({

      type: 'EmptyDagNode',
      kind: 'Embeddable'

    });

    return Embeddable;

  });