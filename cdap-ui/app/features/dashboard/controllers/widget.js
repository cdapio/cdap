/**
 * Widget model & controllers
 */

angular.module(PKG.name+'.feature.dashboard')
  .factory('Widget', function (MyDataSource, myHelpers) {

    function Widget (opts) {
      opts = opts || {};
      this.title = opts.title || 'Widget';

      // Type of widget and the metrics
      this.type = opts.type;
      this.metric = opts.metric || false;
      this.metricAlias =  opts.metricAlias || {};

      // Dimensions and Attributes of a widget
      this.settings = {};
      opts.settings = opts.settings || {}; // Not a required paramter.
      this.settings.color = opts.settings.color;

      if (myHelpers.objectQuery(opts, 'settings', 'size')) {
        this.width = opts.settings.size.width;
        this.height = opts.settings.size.height;
      } else {
        this.width = '';
        this.height = 200;
      }

      this.settings.chartMetadata = {};
      if (opts.settings.chartMetadata) {
        this.settings.chartMetadata = opts.settings.chartMetadata;
      }

      // Should the widget be live or not.
      this.settings.isLive = opts.settings.isLive || false;
      // Based on Live or not what is the interval at which to poll
      // and how should the value be aggregated
      // (if we get 60 values but want to aggregate it to show only 5)
      this.settings.interval = opts.settings.interval;
      this.settings.aggregate = opts.settings.aggregate;
    }

    Widget.prototype.getPartial = function () {
      return '/assets/features/dashboard/templates/widgets/' + this.type + '.html';
    };

    return Widget;
  })

  .controller('DropdownCtrl', function ($scope, $state, $dropdown) {
    $scope.ddWidget = function(event){
      var toggle = angular.element(event.target);
      if(!toggle.hasClass('dropdown-toggle')) {
        toggle = toggle.parent();
      }

      var scope = $scope.$new(),
          dd = $dropdown(toggle, {
            template: 'assets/features/dashboard/templates/partials/wdgt-dd.html',
            animation: 'am-flip-x',
            trigger: 'manual',
            prefixEvent: 'wdgt-tab-dd',
            scope: scope
          });

      dd.$promise.then(function(){
        dd.show();
      });

      scope.$on('wdgt-tab-dd.hide', function () {
        dd.destroy();
      });
    };
  });
