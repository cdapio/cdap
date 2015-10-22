angular.module(PKG.name + '.services')
  .factory('myAlertOnValium', function($alert) {
    var isAnAlertOpened = false,
        alertObj;
     function show(obj) {
       if (!isAnAlertOpened) {
         isAnAlertOpened = true;
         alertObj = $alert(obj);
         alertObj.$scope
          .$on('alert.hide', function() {
            isAnAlertOpened = false;
          });
       }
     }

     return {
       show: show
     };
  });
