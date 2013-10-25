/*
 * Components Index
 */

define(['js/models/all', 'js/controllers/all'],
  function (Models, Controllers) {

    Em.debug('Loading Components');

    return Models.concat(Controllers);

  }
);