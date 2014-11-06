/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($alert) {

  $alert({
    title: 'hello',
    content: 'world!',
    type: 'info'
  });

});



