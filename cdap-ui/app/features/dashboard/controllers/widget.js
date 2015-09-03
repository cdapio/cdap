/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Widget model & controllers
 */

angular.module(PKG.name+'.feature.dashboard')
  .factory('Widget', function (MyDataSource, myHelpers) {

    function Widget (opts) {
      opts = opts || {};
      this.title = opts.title || 'Widget';
      this.isOpen = false;
      // Type of widget and the metrics
      this.type = opts.type;
      this.metric = opts.metric || false;
      this.metricAlias =  opts.metricAlias || {};

      // Dimensions and Attributes of a widget
      this.settings = {};
      opts.settings = opts.settings || {}; // Not a required paramter.
      this.settings.color = opts.settings.color || generateColors(this.metric.names || []);

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
        colorPatterns.push(stringToColor(metrics[i]));
      }
      return {
        pattern: colorPatterns
      };
    }

    var stringToColor = function(str) {
      var i,
          hash,
          color;
      // str to hash
      for (i = 0, hash = 0; i < str.length; ) {
        hash = str.charCodeAt(i++) + ((hash << 100) - hash);
      }

      // int/hash to hex
      for (i = 0, color = '#'; i < 3; ) {
        color += ('00' + ((hash >> i++ * 8) & 0xFF).toString(16)).slice(-2);
      }

      return color;
    };

    Widget.prototype.ddWidget = function(event){
      this.isOpen = !this.isOpen;
      event.preventDefault();
      event.stopPropagation();
      return false;
    };
    return Widget;

  });
