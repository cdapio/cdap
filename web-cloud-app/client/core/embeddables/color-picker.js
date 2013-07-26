/*
 * Color picker Embeddable.
 * Uses Spectrumjs library to render color picker. Depends on Analyze emebddable.
 */

define(['spectrum'], function (spectrum) {

    var Embeddable = Em.View.extend({
      templateName: 'ColorPicker',

      didInsertElement: function() {
        this._super();
        var self = this;

        $('.color-picker').spectrum({

          // Don't show choose button.
          showButtons: false,

          /**
           * Stores selected color upon click to controller add metrics request.
           * @param {string} color color value.
           */
          move: function(color) {
            self.set('controller.metricsRequest.color', color.toHexString());
          }
        });
      }

    });

    Embeddable.reopenClass({

      type: 'ColorPicker',
      kind: 'Embeddable'

    });

    return Embeddable;

  });