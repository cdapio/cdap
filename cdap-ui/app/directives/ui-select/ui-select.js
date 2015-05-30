angular.module(PKG.name + '.commons')
  .directive('myUiSelect', function() {
    return {
      templateUrl: 'ui-select/ui-select.html',
      scope: {
        selectedAdapterDraft: '=',
        onSelect: '&',
        adaptersDraftList: '='
      },
      link: function(scope, element, attrs) {
        var search = element[0].querySelector('input.ui-select-search');
        if (search) {
          search = angular.element(search);
          search.bind('blur', function(event) {
            var value = event.target.value;
            if (value && value.length){
              search.triggerHandler({
                type: 'keydown',
                which: 13, // key code for ENTER key. 
                keyCode: 13
              });
            }
          });
        }
      },
      controller: function($scope) {
        $scope.onDraftChange = function(item, model) {
          $scope.$apply(function(scope) {
            var fn = scope.onSelect();
            if ('undefined' !== typeof fn) {
              fn(item, model);
            }
          });
        }
      }
    };
  });
