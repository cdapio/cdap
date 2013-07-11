/*
 * Analyze tab Embeddable. This manages the graph on the Analyze tab.
 * Depends on Analyze controller and must be embedded inside of it.
 */

define(['../../helpers/chart-helper'], function (chartHelper) {

    var Embeddable = Em.View.extend({
      templateName: 'AnalyzeEmbeddable',

      didInsertElement: function() {
        this._super();

        this.renderCharts();
        this.set('overlays', Em.A([
          $("#analyze-add-metric-widget")
        ]));
        this.set('addMetricButton', $('#analyze-add-metric-button'));
      },

      /**
       * Renders all charts.
       * This observes on controller.data, and changes everytime there is a change in the data.
       */
      renderCharts: function() {
        $("#metrics-explorer-widget").empty();
        var dt = this.get('controller.data');
        var metrics = this.get('controller.metrics');
        new chartHelper.Chart(dt, metrics, 'metrics-explorer-widget');
      }.observes('controller.data'),

      /**
       * Closes open overlays.
       */
      closeOverlays: function() {
        this.get('overlays').hide();
      },

      /**
       *  Opens/closes add metric dialog.
       */
      toggleDialog: function() {
        $("#analyze-add-metric-widget").toggle(this.get('controller.isAddMetricVisible'));
      }.observes('controller.isAddMetricVisible')

    });

    Embeddable.reopenClass({

      type: 'Analyze',
      kind: 'Embeddable'

    });

    return Embeddable;

  });