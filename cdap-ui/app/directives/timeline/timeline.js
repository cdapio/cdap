/*
 * Copyright © 2016 Cask Data, Inc.
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
      sliderX,
      timelineStack,
      startTime,
      endTime,
      pinOffset,
      circleTooltip,
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
      scrollNeedle;

  //Initialize charting
  scope.initialize = () => {

    //If chart already exists, remove it
    if(timescaleSvg){
      d3.selectAll('.timeline-container svg > *').filter((d) => {
        return (typeof d === 'undefined') || (d.attr('class') !== 'search-circle');
      }).remove();
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
    sliderLimit = maxRange;
    pinOffset = 13;
    pinX = 0;
    sliderX = 0;
    timelineStack = {};
    sliderHandle = undefined;
    pinHandle = undefined;
    sliderBrush = undefined;
    scrollPinBrush = undefined;
    scrollNeedle = undefined;
    xScale = undefined;
    slide = undefined;
    slider = undefined;
    timescaleSvg = undefined;
    scrollPinSvg = undefined;
    xAxis = undefined;
    sliderBar = undefined;
    timelineData = scope.metadata;

    scope.plot();
  };

  /* ------------------- Plot Function ------------------- */
  scope.plot = function(){

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

    generateEventCircles();
    renderBrushAndSlider();
  };

  // -------------------------Build Brush / Sliders------------------------- //
  function renderBrushAndSlider(){

    timescaleSvg.append('g')
      .attr('class', 'xaxis-bottom')
      .attr('transform', 'translate(' + ( (paddingLeft + paddingRight) / 2) + ',' + (height - 20) + ')')
      .call(xAxis);

    //attach handler to brush
    sliderBrush = d3.svg.brush()
        .x(xScale)
        .on('brush', function(){
          if(d3.event.sourceEvent) {
            let val = d3.mouse(this)[0];
            if(val < 0){
              val = 0;
            }
            if(val > maxRange){
              val = maxRange;
            }
            sliderHandle.attr('x', val);
            sliderBar.attr('d', 'M0,0V0H' + val + 'V0');
            pinHandle.attr('x', val-pinOffset+1);
            scrollNeedle.attr('x1', val + 8)
                        .attr('x2', val + 8);
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
            updateSlider(val);
            pinHandle.attr('x', val-pinOffset+1);
            scrollNeedle.attr('x1', val + 8)
                        .attr('x2', val + 8);
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
      .attr('width', 8)
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

    pinHandle = slider.append('svg:image')
      .attr('width', 40)
      .attr('height', 60)
      .attr('xlink:href', '/assets/img/scrollPin.svg')
      .attr('x', xValue - pinOffset)
      .attr('y', 0);

    scrollNeedle = slide.append('line')
      .attr('x1', xValue + pinOffset - 6)
      .attr('x2', xValue + pinOffset - 6)
      .attr('y1', -10)
      .attr('y2', 40)
      .attr('stroke-width', 1)
      .attr('stroke', 'grey');
  }

  scope.updatePinScale = function (val) {
    if(pinHandle !== undefined){
      pinHandle.attr('x', xScale(Math.floor(val/1000)));
      scrollNeedle.attr('x', xScale(Math.floor(val/1000)));
    }
  };

  scope.renderSearchCircles = (searchTimes) => {

    d3.selectAll('.search-circle').remove();

    angular.forEach(searchTimes, (value) => {
      timescaleSvg.append('circle')
        .attr('cx', xScale(value))
        .attr('cy', 35)
        .attr('r', 2)
        .attr('class', 'search-circle');
    });
  };

  function updateSlider(val) {
    if(val < 0){
      val = 0;
    }
    if(val > sliderLimit){
      val = sliderLimit;
    }
    sliderX = val;

    sliderHandle.attr('x', val);
    sliderBar.attr('d', 'M0,0V0H' + val + 'V0');
    scope.Timeline.updateStartTimeInStore(xScale.invert(val));
  }

  scope.updateSlider = updateSlider;

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

    //Subroutine
    const mapEvents = (type, data) => {
      let typeMap;
      if(type === 'warn'){
        typeMap = warningMap;
      } else if(type === 'error'){
        typeMap = errorMap;
      }

      for(let k = 0; k < data.length; k++){
        let currentItem = data[k];
        let xVal = Math.floor(xScale(currentItem.time * 1000));
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

    const circleTooltipHoverOn = function(position, errors, warnings) {
      let date = xScale.invert(position).toString();
      date = date.substring(0, date.length - 14);

      circleTooltip.transition()
          .duration(200)
          .style('opacity', 0.9);
      circleTooltip.html(date + '<br/>Errors: ' + errors + '<br/>Warnings: ' + warnings)
         .style('left', (d3.event.pageX + 5) + 'px')
         .style('top', (d3.event.pageY - 30) + 'px');
    };

    const circleTooltipHoverOff = () =>{
      circleTooltip.transition()
        .duration(500)
        .style('opacity', 0);
    };

    let timelineHash = {};
    for(var key in errorMap){
      if(errorMap.hasOwnProperty(key)){
        timelineHash[key] = {
          'errors' : errorMap[key]
        };
      }
    }
    for(var keyTwo in warningMap){
      if(warningMap.hasOwnProperty(keyTwo)){
        if(!timelineHash[keyTwo]) {
          timelineHash[keyTwo] = {
            'warnings' : warningMap[keyTwo]
          };
        } else {
          timelineHash[keyTwo].warnings = warningMap[keyTwo];
        }
      }
    }
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

        let eventCount = errorCount + warningCount;
        let circleTooltipHover = circleTooltipHoverOn.bind(this, keyThree, !timelineHash[keyThree].errors ? 0 : timelineHash[keyThree].errors, !timelineHash[keyThree].warnings ? 0 : timelineHash[keyThree].warnings);

        for(var num = 4; num >= 0 && (errorCount > 0 || warningCount > 0); num--){
          if(errorCount > 0){
            if(eventCount >= 5){
              timescaleSvg.append('circle')
                .attr('cx', keyThree)
                .attr('cy', (num+1) * 7)
                .attr('r', 2)
                .attr('class', 'red-circle')
                .on('mouseover', circleTooltipHover)
                .on('mouseout', circleTooltipHoverOff);
            } else {
              timescaleSvg.append('circle').attr('cx', keyThree).attr('cy', (num+1) * 7).attr('r', 2).attr('class', 'red-circle');
            }
            errorCount--;
          } else if(warningCount > 0){
            if(eventCount >= 5){
              timescaleSvg.append('circle')
                .attr('cx', keyThree)
                .attr('cy', (num+1) * 7)
                .attr('r', 2)
                .attr('class', 'yellow-circle')
                .on('mouseover', circleTooltipHover)
                .on('mouseout', circleTooltipHoverOff);
            } else {
              timescaleSvg.append('circle').attr('cx', keyThree).attr('cy', (num+1) * 7).attr('r', 2).attr('class', 'yellow-circle');
            }
            warningCount--;
          }
        }
      }
    }
  };
  scope.generateEventCircles = generateEventCircles;
}

angular.module(PKG.name + '.commons')
.directive('myTimeline', function() {
  return {
    templateUrl: 'timeline/timeline.html',
    scope: {
      timelineData: '=?',
      namespaceId: '@',
      appId: '@',
      programType: '@',
      programId: '@',
      runId: '@'
    },
    link: link,
    bindToController: true,
    controller: 'TimelineController',
    controllerAs: 'Timeline'
  };
});
