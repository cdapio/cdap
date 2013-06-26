/*
 * Analyze Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({
    typesBinding: 'model.types',

    load: function (id) {

      console.log('load called');

    },

    updateStats: function () {
      var self = this;

      // Update timeseries data for current flow.
      C.get.apply(C, this.get('model').getUpdateRequest());

    },

    unload: function () {


    },

    delete: function () {

    }

  });

  Controller.reopenClass({
    type: 'Analyze',
    kind: 'Controller'
  });

  return Controller;

});