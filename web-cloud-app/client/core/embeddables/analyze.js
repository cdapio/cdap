/*
 * Analyze tab Embeddable. This manages the graph on the Analyze tab.
 * Depends on Analyze controller and must be embedded inside of it.
 */

define(['../../helpers/chart-helper'], function (chartHelper) {

    var seriesOptions = {
      marker: {
        enabled: false,
        states: {
          hover: {
            enabled: false
          }
        }
      }
    };

    var Embeddable = Em.View.extend({

      elementId: 'metrics-explorer-widget',

      build: function () {

        Highcharts.setOptions({
            global: {
                useUTC: false
            }
        });

        var chart;
        chart = $(this.get('element')).highcharts({
            chart: {
                type: 'spline',
                animation: Highcharts.svg
            },
            title: {
                text: null
            },
            xAxis: {
                type: 'datetime',
                tickPixelInterval: 100
            },
            yAxis: {
                title: {
                    text: null
                },
                plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#DDDDDD'
                }]
            },
            tooltip: {
              formatter: function() {
                return '<b>'+ this.series.name +'</b>: <b>' +
                Highcharts.numberFormat(this.y, 2) + '</b><br/>'+
                Highcharts.dateFormat('%Y-%m-%d %H:%M:%S', this.x);
              }
            },
            legend: { enabled: false },
            exporting: { enabled: false },
            series: []
        }).highcharts();

        this.set('chart', chart);

      },

      update: function () {

        function diff (current, more) {

          if (!current.length) {
            return more;
          }

          var x = current[current.length - 1].x, index = 0;
          var i = more.length;
          while (i--) {
            if (more[i].x <= x) {
              index = i;
              break;
            }
          }
          return more.slice(index + 1);

        }

        var current = this.get('chart.series');
        var more = this.get('controller.series');

        var i = more.length, j, k, found;
        while (i--) {

          j = current.length, found = false;
          while (j--) {
            if (current[j].name === more[i].name) {

              var data = diff(current[j].data, more[i].data);

              for (k = 0; k <= data.length - 1; k++) {
                current[j].addPoint([data[k].x, data[k].y], false, true);
              }
              found = true;
            }
          }
          if (!found) {
            $.extend(more[i], seriesOptions);
            this.get('chart').addSeries(more[i]);
          }

        }

        i = current.length;
        while (i--) {

          j = more.length, found = false;
          while (j--) {
            if (current[i].name === more[j].name) {
              found = true;
            }
          }
          if (!found) {
            current[i].remove();
          }
        }

        this.get('chart').redraw();

      }.observes('controller.series'),

      didInsertElement: function () {
        this.build();
      }

    });

    Embeddable.reopenClass({
      type: 'Analyze',
      kind: 'Embeddable'
    });

    return Embeddable;

  });