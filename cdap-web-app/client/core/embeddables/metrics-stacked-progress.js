/*
 * Stacked progress Embeddable.
 */

define([], function () {

    var Embeddable = Em.View.extend({
        templateName: 'SparkMetricsProgressBar',
        didInsertElement: function() {
            this._super();
            var progress = ($(this.get('element')).find('.progress'));
            //init tooltip
            progress.find('div').tooltip({trigger: 'manual', animation: false}).tooltip('show');

            this.set('progress', progress);
            this.addObserver('controller.model.metricsUpdated', this, this.rescaleLineSizes);
        },

        rescaleLineSizes: function () {
            var metricsData = this.get('controller.model.metricsData');
            var progress = this.get('progress');
            var failedStages = metricsData.schedulerFailedStages;
            var runningStages = metricsData.schedulerRunningStages;
            var waitingStages = metricsData.schedulerWaitingStages;
            var totalStages = failedStages + runningStages + waitingStages;
            //default values if totalStages == 0
            var runningRatio = 33.3,
                failedRatio = 33.3,
                waitingRatio = 33.3;
            if (totalStages != 0) {
                //if some of stages have zero value then we have to assign 3% (min ratio) to this stage ratio and
                // reduce ratio of another stages
                checkForZero(runningStages, failedStages, waitingStages);
                runningRatio = getRatio(runningStages);
                failedRatio = getRatio(failedStages);
                waitingRatio = getRatio(waitingStages);
            }

            progress.find('.running').html(runningStages).css('width', runningRatio + '%');
            progress.find('.failed').html(failedStages).css('width', failedRatio + '%');
            progress.find('.waiting').html(waitingStages).css('width', waitingRatio + '%');
            progress.find('div').tooltip('hide').tooltip('show');
            this.set('controller.model.metricsUpdated', false);

            /*
             In stages status line blocks we use minimum width 3% so '0' value can be displayed correct.
             How recalculation works:
             For example,
             if runningStage = 5, failedRatio = 5, waitingRatio = 0
             then we have ratio
             runningRatio = 50%
             failedRatio = 50%
             waitingRatio = 3% (because minimum ratio must be 3% for correct drawing)
             so, we have total sum 103% that can broke our drawing
             To resolve this we can reduce ration of running and failed stage to 48.5% (50% - 3%/2)
             and we can draw correct proportions of stage blocks.
             Here we try to find zero values, and each zero will increase totalStages for 3% of it value

             x = 5
             y = 5
             z = 0
             minPercent = 3
             zeroMultiplier = 1
             totalStages = x + y + z = 10
             totalStages += totalStages / (100 - minPercent*zeroMultiplier) * minPercent
             (numbers: totalStages += 10 / (100 - 3*1) * 3 = 10,309)
             so now we can use formula for percent ratio
             ratioX = x / (totalStages / 100) = 48.5
             ratioY = y / (totalStages / 100) = 48.5
             ratioZ = 3 (as minimum)

             If we have several zero values
             x = 2
             y = 0
             z = 0
             minPercent = 3
             zeroMultiplier = 1
             totalStages = x + y + z = 2
             sum for each zero
             totalStages += totalStages / (100 - minPercent*zeroMultiplier) * minPercent = 2,063
             zeroMultiplier++
             totalStages += totalStages / (100 - minPercent*zeroMultiplier) * minPercent = 2,1341
             (numbers: totalStages += 2,063 / (100 - 3*2) * 3 = 2,1341)
             ratioX = x / (totalStages / 100) = 94
             ratioY = 3 (as minimum)
             ratioZ = 3 (as minimum)
             */
            function checkForZero() {
                var multiplier = 1;
                var minPercent = 3;
                for (var i = 0; i < arguments.length; i++) {
                    if (arguments[i] === 0) {
                        //append extra width to status line that we can draw 3% min width of status block
                        totalStages += (totalStages / (100 - minPercent * multiplier++)) * minPercent;
                    }
                }
            }

            function getRatio(val) {
                if (val === 0) {
                    //minimum 3% status line width
                    return 3;
                }
                return val / (totalStages / 100);
            }
        }
    });

    Embeddable.reopenClass({
        type: 'StackedProgress',
        kind: 'Embeddable'
    });

    return Embeddable;
});
