/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $alert, mySocket, MY_SOCKET_EVENT) {

  window.mySocket = mySocket;

  $scope.sendMsg = function(msg) {
    console.log('sendMsg', msg);
    mySocket.send(msg);
  };

  $scope.$on(MY_SOCKET_EVENT.message, function (e, data) {

    console.log(e, data);

    $alert({
      title: 'socket message',
      content: data,
      type: 'info'
    });

    // $scope.something = '';
  });

});



