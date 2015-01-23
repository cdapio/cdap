angular.module(PKG.name + '.commons')
  .directive('myMetricPicker', function (MyDataSource, $stateParams, $log) {

    var dSrc = new MyDataSource();

    function MetricPickerCtrl ($scope) {

      console.log($stateParams);
      var ns = [$stateParams.namespace,'namespace'].join(' ');

      $scope.available = {
        contexts: [],
        types: ['system', ns],
        names: []
      };

      $scope.metric = {
        context: '',
        type: ns,
        name: ''
      };

    }

    return {
      restrict: 'E',

      require: 'ngModel',

      scope: {},

      templateUrl: 'metric-picker/metric-picker.html',

      controller: MetricPickerCtrl,

      link: function (scope, elem, attr, ngModel) {

        function getContext () {
          var context;

          if(scope.metric.type==='system') {
            context = 'system';
          }
          else {
            context = $stateParams.namespace;
          }

          if(!context) { // should never happen, except on directive playground
            context = 'default';
            $log.warn('metric-picker using default namespace as context!');
          }

          if(scope.metric.context) {
            context += '.' + scope.metric.context;
          }

          return context;
        }

        function fetchAhead () {
          var context = getContext();

          scope.available.contexts = dSrc.request(
            {
              method: 'POST',
              _cdapPath: '/metrics/search?target=childContext' +
                '&context=' + encodeURIComponent(context)
            }
          );

          scope.available.names = dSrc.request(
            {
              method: 'POST',
              _cdapPath: '/metrics/search?target=metric' +
                '&context=' + encodeURIComponent(context)
            }
          );

        }

        var onBlurHandler = ngModel.$setTouched.bind(ngModel);
        elem.find('button').on('blur', onBlurHandler);
        elem.find('input').on('blur', onBlurHandler);

        scope.$watchCollection('metric', function (newVal, oldVal) {

          ngModel.$validate();

          if(newVal.type !== oldVal.type) {
            scope.metric.context = '';
            scope.metric.name = '';
            fetchAhead();
            return;
          }

          if(newVal.context !== oldVal.context) {
            scope.metric.name = '';
            fetchAhead();
          }

          if(newVal.context && newVal.name) {
            ngModel.$setViewValue([
              '/metrics',
              getContext(),
              newVal.name
            ].join('/'));
          }
          else {
            if(ngModel.$dirty) {
              ngModel.$setViewValue(null);
            }

          }

        });

        fetchAhead();
      }
    };
  });
