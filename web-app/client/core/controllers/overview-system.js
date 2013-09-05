/*
 * System Overview Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    elements: Em.Object.create(),
    counts: Em.Object.create(),
    __remaining: -1,

    aggregates: Em.Object.create(),
    timeseries: Em.Object.create(),
    value: Em.Object.create(),

    load: function () {

    },

    unload: function () {

    }

  });

  Controller.reopenClass({
    type: 'OverviewSystem',
    kind: 'Controller'
  });

  return Controller;

});