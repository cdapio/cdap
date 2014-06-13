/*
 * Service Log Controller
 */

define(['core/controllers/runnable-log'], function (RunnableLogController) {

  var Controller = RunnableLogController.extend({

    init: function () {

      this.set('expectedPath', 'Service.Log');
    }

  });

  Controller.reopenClass({
    type: 'ServiceLog',
    kind: 'Controller'
  });

  return Controller;

});