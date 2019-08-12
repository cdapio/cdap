/*
 * Copyright © 2017 Cask Data, Inc.
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

function link (scope, element) {

  //Globals
  let width,
      height,
      paddingLeft,
      paddingRight,
      maxRange,
      sliderLimit,
      pinX,
      timelineStack,
      startTime,
      endTime,
      pinOffset,
      circleTooltip,
      handleWidth,
      totalCount,
      firstRun = true;

  //Components
  let sliderHandle,
      pinHandle,
      sliderBrush,
      scrollPinBrush,
      xScale,
      timelineData,
      slide,
      slider,
      timescaleSvg,
      scrollPinSvg,
      xAxis,
      sliderBar,
      scrollNeedle,
      tooltipDiv;

  //Initialize charting
  scope.initialize = () => {

    // If chart already exists, remove it
    if(timescaleSvg){
      d3.selectAll('.timeline-container svg > *:not(.search-circle)').remove();
      timescaleSvg.remove();
    }

    if(scrollPinSvg){
      scrollPinSvg.remove();
    }

    width = element.parent()[0].offsetWidth;
    height = 50;
    paddingLeft = 15;
    paddingRight = 15;
    maxRange = width - paddingRight + 8;
    handleWidth = 8;
    // maxRange = width - width of handle - width of needle;
    maxRange = width - 12;
    sliderLimit = maxRange;
    pinOffset = 13;
    pinX = 0;
    timelineStack = {};
    timelineData = scope.metadata;
    totalCount = 0;
    scope.plot();
  };

  /* ------------------- Plot Function ------------------- */
  scope.plot = function(){

    if(typeof timelineData.qid === 'undefined') {
      return;
    }

    startTime = timelineData.qid.startTime*1000;
    endTime = timelineData.qid.endTime*1000;

    timescaleSvg = d3.select('.timeline-log-chart')
      .append('svg')
      .attr('width', width)
      .attr('height', height);

    //Set the Range and Domain
    xScale = d3.time.scale().range([0, (maxRange)]);
    xScale.domain([startTime, endTime]);

    var customTimeFormat = d3.time.format.multi([
      ['.%L', function(d) { return d.getMilliseconds(); }],
      [':%S', function(d) { return d.getSeconds(); }],
      ['%H:%M', function(d) { return d.getMinutes(); }],
      ['%H:%M', function(d) { return d.getHours(); }],
      ['%a %d', function(d) { return d.getDay() && d.getDate() !== 1; }],
      ['%b %d', function(d) { return d.getDate() !== 1; }],
      ['%B', function(d) { return d.getMonth(); }],
      ['%Y', function() { return true; }]
    ]);

    // Define the div for the circles-tooltip
    circleTooltip = d3.select('body').append('div')
      .attr('class', 'circle-tooltip')
      .style('opacity', 0);

    xAxis = d3.svg.axis().scale(xScale)
      .orient('bottom')
      .innerTickSize(-40)
      .outerTickSize(0)
      .tickPadding(7)
      .ticks(8)
      .tickFormat(customTimeFormat);

    getLogStats();
    generateEventCircles();
    renderBrushAndSlider();
  };

  // -------------------------Build Brush / Sliders------------------------- //
  function renderBrushAndSlider(){

    timescaleSvg.append('g')
      .attr('class', 'xaxis-bottom')
      .attr('transform', 'translate(' + handleWidth + ', ' + (height - 20) + ')')
      .call(xAxis);

    //attach handler to brush
    sliderBrush = d3.svg.brush()
        .x(xScale)
        .on('brush', function(){
          if(d3.event.sourceEvent) {
            let val = d3.mouse(this)[0];

            updateSlider(val);
          }
        })
        .on('brushend', function() {
          if(d3.event.sourceEvent){
            let val = d3.mouse(this)[0];
            if(val < 0){
              val = 0;
            }
            if(val > maxRange){
              val = maxRange;
            }

            //Snap the slider into place
            val = xScale(Math.floor(xScale.invert(val)/1000)*1000);
            updateSlider(val);

            //Update scroll position before updating starttime
            scope.Timeline.updateScrollPositionInStore(Math.floor(xScale.invert(val)));
            scope.Timeline.updateStartTimeInStore(Math.floor(xScale.invert(val)));
          }
       });

    //Creates the top slider and trailing dark background
    sliderBar = timescaleSvg.append('g')
      .attr('class', 'slider leftSlider')
      .call(d3.svg.axis()
        .scale(xScale)
        .tickSize(0)
        .tickFormat(''))
      .select('.domain')
      .attr('class', 'fill-bar');

    slide = timescaleSvg.append('g')
          .attr('class', 'slider')
          .attr('transform' , 'translate(0,10)')
          .call(sliderBrush);

    if(firstRun){
      firstRun = false;
      scope.sliderBarPositionRefresh = xScale.invert(0);
      scope.Timeline.updateStartTimeInStore(xScale.invert(0));
    }
    let xValue = xScale(scope.sliderBarPositionRefresh);
    if(xValue < 0 || xValue > maxRange){
      xValue = 0;
    }

    sliderBar.attr('d', 'M0,0V0H' + xValue + 'V0');

    sliderHandle = slide.append('svg:image')
      .attr('width', handleWidth)
      .attr('height', 52)
      .attr('xlink:href', '/assets/img/sliderHandle.svg')
      .attr('x', xValue-1)
      .attr('y', -10);

    //Append the Top slider
    scrollPinBrush = d3.svg.brush()
        .x(xScale);

    scrollPinSvg = d3.select('.top-bar').append('svg')
        .attr('width', width)
        .attr('height', 20);

    scrollPinSvg.append('g')
        .attr('class', 'xaxis-top')
        .call(d3.svg.axis()
          .scale(xScale)
          .orient('bottom'))
      .select('.domain')
      .select( function() {
        return this.parentNode.appendChild(this.cloneNode(true));
      });

    slider = scrollPinSvg.append('g')
        .attr('class', 'slider')
        .attr('width', width)
        .call(scrollPinBrush);

    slider.select('.background')
      .attr('height', 15);

    let thePinScrollPosition = xScale(scope.pinScrollingPosition);

    pinHandle = slider.append('svg:image')
      .attr('width', 40)
      .attr('height', 60)
      .attr('xlink:href', '/assets/img/scrollPin.svg')
      .attr('x', thePinScrollPosition - pinOffset)
      .attr('y', 0)
      .on('mouseover', function() {
        tooltipDiv = d3.select('body').append('div')
          .attr('class', 'tooltip')
          .style('opacity', 0);

        //Reposition tooltip if overflows on the side of the page ; tooltip width is 250
        let overflowOffset = xScale(scope.pinScrollingPosition) + 250 > maxRange ? 250 : 0;

        tooltipDiv.transition()
          .duration(200)
          .style('opacity', 0.9)
          .attr('class', 'timeline-tooltip');

        let tooltipTime = scope.pinScrollingPosition;

        if(!(tooltipTime instanceof Date)){
          tooltipTime = new Date(tooltipTime);
        }

        tooltipDiv.html(tooltipTime)
          .style('left', (d3.event.pageX - overflowOffset) + 'px')
          .style('top', (d3.event.pageY - 28) + 'px');
      })
      .on('mouseout', function() {
        d3.selectAll('.timeline-tooltip').remove();
      });

    scrollNeedle = slide.append('line')
      .attr('x1', thePinScrollPosition + pinOffset - 6)
      .attr('x2', thePinScrollPosition + pinOffset - 6)
      .attr('y1', -10)
      .attr('y2', 40)
      .attr('stroke-width', 1)
      .attr('stroke', 'grey');
  }

  scope.updateSliderHandle = (startTime) => {
    if(typeof xScale !== 'undefined'){
      updateSlider(xScale(startTime));
    }
  };

  scope.updatePin = function () {
    if (typeof xScale === 'undefined') {
      return;
    }
    let xPositionVal = xScale(scope.pinScrollingPosition);

    if(typeof pinHandle !== 'undefined'){

      if(totalCount === 0){
        xPositionVal = 0;
      }

      pinHandle.attr('x', xPositionVal - pinOffset + 1);
      scrollNeedle.attr('x1', xPositionVal + 8)
        .attr('x2', xPositionVal + 8);
    }
  };

  const movePin = (val) => {
    if(typeof pinHandle !== 'undefined'){
      pinHandle.attr('x', val - pinOffset + 1);
      scrollNeedle.attr('x1', val + 8)
                  .attr('x2', val + 8);
    }
  };

  scope.renderSearchCircles = (searchTimes) => {

    d3.selectAll('.search-circle').remove();

    angular.forEach(searchTimes, (value) => {
      timescaleSvg.append('circle')
        .attr('cx', xScale(value) + handleWidth)
        .attr('cy', 35)
        .attr('r', 2)
        .attr('class', 'search-circle');
    });
  };

  function updateSlider(val) {

    if(typeof sliderHandle === 'undefined'){
      return;
    }

    if(val < 0){
      val = 0;
    }
    if(val > maxRange){
      val = maxRange;
    }

    //Keep the pin current
    if(typeof pinHandle !== 'undefined'){
      if(pinHandle.attr('x') < val){
        movePin(val);
      }
    }
    sliderHandle.attr('x', val);
    sliderBar.attr('d', 'M0,0V0H' + val + 'V0');
  }

  scope.updateSlider = updateSlider;

  const getLogStats = () => {
    let errorCount = 0;
    let warningCount = 0;

    if(timelineData.qid.series.length > 0){
      for(let i = 0; i < timelineData.qid.series.length; i++){

        for(let j = 0; j < timelineData.qid.series[i].data.length; j++){
          let currentItem = timelineData.qid.series[i].data[j];
          let numEvents = currentItem.value;

          totalCount += numEvents;

          if(timelineData.qid.series[i].metricName === 'system.app.log.error'){
            errorCount += numEvents;
          } else if(timelineData.qid.series[i].metricName === 'system.app.log.warn'){
            warningCount += numEvents;
          }
        }
      }
    }

    scope.Timeline.updateTotalLogsInStore(totalCount);
    scope.Timeline.updateTotalErrorsInStore(errorCount);
    scope.Timeline.updateTotalWarningsInStore(warningCount);
  };

  const generateEventCircles = () => {

    //Clears the tooltip regularly
    d3.selectAll('.circle-tooltip').remove();

    circleTooltip = d3.select('body').append('div')
      .attr('class', 'circle-tooltip')
      .style('opacity', 0);

    timelineStack = {};
    let errorMap = {};
    let warningMap = {};
    timelineData = scope.metadata;

    const mapEvents = (type, data) => {
      let typeMap;
      if(type === 'warn'){
        typeMap = warningMap;
      } else if(type === 'error'){
        typeMap = errorMap;
      }

      for(let k = 0; k < data.length; k++){
        let currentItem = data[k];
        let xVal = (currentItem.time * 1000);
        typeMap[xVal] = currentItem.value;
      }
      return typeMap;
    };

    if(timelineData.qid.series.length > 0){
      for(let i = 0; i < timelineData.qid.series.length; i++){
        switch(timelineData.qid.series[i].metricName){
          case 'system.app.log.error':
            errorMap = mapEvents('error', timelineData.qid.series[i].data);
            break;
          case 'system.app.log.warn':
            warningMap = mapEvents('warn', timelineData.qid.series[i].data);
            break;
          default:
            break;
        }
      }
    }

    const circleTooltipHoverOn = function(position, errors = 0, warnings = 0) {
      position = parseInt(position, 10);
      let date = new Date(position);
      date = scope.moment(date).format('MMMM Do YYYY, h:mm:ss a');

      let tooltipOverflowOffset = xScale(position) + 180 > maxRange ? 180 : 0;

      circleTooltip.transition()
        .duration(200)
        .style('opacity', 0.9);
      circleTooltip.html(date + '<br/>Errors: ' + errors + '<br/>Warnings: ' + warnings)
        .style('left', (d3.event.pageX - tooltipOverflowOffset + 5) + 'px')
        .style('top', (d3.event.pageY - 30) + 'px');
    };

    const circleTooltipHoverOff = () =>{
      circleTooltip.transition()
        .duration(500)
        .style('opacity', 0);
    };

    let timelineHash = {};
    Object.keys(errorMap)
      .forEach(error => {
        timelineHash[error] = {
          'errors' : errorMap[error]
        };
      });
    Object.keys(warningMap)
      .forEach(warning => {
        timelineHash[warning] = timelineHash[warning] || {};
        timelineHash[warning] = Object.assign({}, timelineHash[warning], {
          'warnings' : warningMap[warning]
        });
      });

    for(var keyThree in timelineHash){
      if(timelineHash.hasOwnProperty(keyThree)){
        let errorCount = 0;
        let warningCount = 0;

        if(timelineHash[keyThree].errors){
          errorCount = timelineHash[keyThree].errors;
        }
        if(timelineHash[keyThree].warnings){
          warningCount = timelineHash[keyThree].warnings;
        }

        let circleTooltipHover = circleTooltipHoverOn.bind(
          this, keyThree,
          timelineHash[keyThree].errors,
          timelineHash[keyThree].warnings
        );

        for(var verticalStackSize = 4; verticalStackSize >= 0 && (errorCount > 0 || warningCount > 0); verticalStackSize--){
          if(errorCount > 0){
              timescaleSvg.append('circle')
                .attr('cx', xScale(keyThree) + handleWidth)
                //Value '-2' prevents circles from overlapping with timestamps
                .attr('cy', (verticalStackSize+1) * 7 - 2)
                .attr('r', 2)
                .attr('class', 'red-circle')
                .on('mouseover', circleTooltipHover)
                .on('mouseout', circleTooltipHoverOff);
                errorCount--;
          } else if(warningCount > 0){
              timescaleSvg.append('circle')
                .attr('cx', xScale(keyThree) + handleWidth)
                .attr('cy', (verticalStackSize+1) * 7 - 2)
                .attr('r', 2)
                .attr('class', 'yellow-circle')
                .on('mouseover', circleTooltipHover)
                .on('mouseout', circleTooltipHoverOff);
                warningCount--;
          }
        }
      }
    }

    scope.renderSearchCircles(scope.searchResultTimes);
  };


  scope.generateEventCircles = generateEventCircles;
}

angular.module(PKG.name + '.commons')
.directive('myTimelinePreview', function() {
  return {
    templateUrl: 'timeline/timeline.html',
    scope: {
      timelineData: '=?',
      namespaceId: '@',
      previewId: '@'
    },
    link: link,
    bindToController: true,
    controller: 'TimelinePreviewController',
    controllerAs: 'Timeline'
  };
});
