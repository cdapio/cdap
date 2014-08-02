/*
 * Userservice Log Controller
 */

define(['core/controllers/runnable-log'], function (RunnableLogController) {

  var Controller = RunnableLogController.extend({

    needs: ['Userservice'],
    init: function () {
      this.set('expectedPath', 'Userservice.Log');
    },

  });

  Controller.reopenClass({

    type: 'UserserviceLog',
    kind: 'Controller'

  });

  return Controller;

});