/*
 * Flow Config Controller
 */

define(['core/controllers/runnable-config'], function (RunnableConfigController) {

  var Controller = RunnableConfigController.extend({

    /*
     * This syntax makes the WorkflowStatus controller available to this controller.
     * This allows us to access the workflow model that has already been loaded.
     *
     * RunnableConfigController uses this value to do its work. Take a look there.
     *
     */
    needs: ['WorkflowStatus']

  });

  Controller.reopenClass({

    type: 'WorkflowStatusConfig',
    kind: 'Controller'

  });

  return Controller;

});