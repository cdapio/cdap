/*
 * Event modal. This is the same as a modal but gives a way to optionally fire functions after modal
 * closes.
 */

define([
  'core/lib/text!core/partials/modal.html', 'core/embeddables/modal'
  ], function (Template, Modal) {

    var CONFIG_ERR = 'Invalid config for event modal.';

    var Embeddable = Modal.extend({
      elementId: 'eventmodal-from-dom',
      show: function (config) {
        var self = this;

        if (typeof config !== 'object') {
          throw CONFIG_ERR;
        } else if (!('title' in config) || !('body' in config)) {
          throw CONFIG_ERR;
        }
        this.set('title', config.title);
        this.set('body', config.body);

        if (!('callback' in config)) {
          config.nocancel = true;
        }

        this.set('nocancel', config.nocancel);


        this.set('confirmed', function () {
          self.hide();
          if ('callback' in config && typeof config.callback === 'function') {
            config.callback();
          }
        });

        if ('onHideCallback' in config) {
          this.set('onHideCallback', config.onHideCallback);
        }

        var el = $(this.get('element'));
        el.modal('show');

      },
      hide: function (callback) {

        var el = $(this.get('element'));

        if (typeof callback === "function") {
          el.on('hidden', function () {
            el.off('hidden');
            callback();
          });
        }

        el.modal('hide');

        if (typeof this.get('onHideCallback') === 'function') {
          this.get('onHideCallback')();
        }

      }

    });

    Embeddable.reopenClass({
      type: 'EventModal',
      kind: 'Embeddable'
    });

    return Embeddable;

  });