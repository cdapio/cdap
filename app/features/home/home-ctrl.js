/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($alert, mySocket) {

  $alert({
    title: 'hello',
    content: 'world!',
    type: 'info'
  });

});



