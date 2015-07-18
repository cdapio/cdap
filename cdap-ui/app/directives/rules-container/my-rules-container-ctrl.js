angular.module(PKG.name + '.commons')
  .controller('MyRulesContainerCtrl', function($scope) {
    this.fieldObj = $scope.fieldObj;
    this.fieldObj.rules.push({
      name: 'isGreaterThan'
    });
    this.addRule = function() {
      this.fieldObj.rules.push({
        name: 'isGreaterThan'
      });
    };
    this.deleteRule = function(index) {
      this.fieldObj.rules.splice(index, 1);
      if (!this.fieldObj.rules.length) {
        this.fieldObj.rules.push({
          name: 'isGreaterThan'
        });
      }
    };

    this.onDelete = function() {
      var onDeleteFunction = $scope.onDelete();
      var onDeleteContext = $scope.onDeleteContext;
      if (typeof onDeleteFunction !== 'undefined') {
        onDeleteFunction.call(onDeleteContext, this.fieldObj);
      }
    };
  });
