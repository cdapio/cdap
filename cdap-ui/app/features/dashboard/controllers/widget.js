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
      this.settings.color = opts.settings.color || generateColors(this.metric.names);

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

    function generateColors(metrics) {
      var colorPatterns = [];
      for (var i=0; i <metrics.length; i++) {
        colorPatterns.push(stringToColour(metrics[i]));
      }
      return {
        pattern: colorPatterns
      };
    }

    var stringToColour = function(str) {

      // str to hash
      for (var i = 0, hash = 0;
            i < str.length;
            hash = str.charCodeAt(i++) + ((hash << 100) - hash)
          );

      // int/hash to hex
      for (var i = 0, colour = "#";
            i < 3;
            colour += ("00" + ((hash >> i++ * 8) & 0xFF).toString(16)).slice(-2)
          );

      return colour;
    }

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
