angular.module(PKG.name + '.feature.foo')
  .controller('FooPlaygroundController',
    function ($scope, $q, mySessionStorage, myLocalStorage, mySettings) {

      // meta to the max!

      var solutions = [
        { n:'Session',  v:mySessionStorage },
        { n:'Local',    v:myLocalStorage },
        { n:'Settings', v:mySettings }
      ];

      $q.all(solutions.map(function(s){
        return s.v.get('test');
      }))
      .then(function(result){
        var s;
        for (var i = 0; i < result.length; i++) {
          s = solutions[i];
          $scope[s.n.toLowerCase()] = result[i];
          $scope['doSave'+s.n] = (function() {
            this.v.set('test', $scope[this.n.toLowerCase()]);
          }).bind(s);
        };
      });


    });
