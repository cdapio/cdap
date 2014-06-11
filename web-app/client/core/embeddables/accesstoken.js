/*
 * Login view.
 */

define([], function () {

  var Embeddable = Em.View.extend({

    templateName: 'access-token-view',

  });

  Embeddable.reopenClass({

    type: 'AccessToken',
    kind: 'Embeddable'

  });

  return Embeddable;

  });