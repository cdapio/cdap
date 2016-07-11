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

  let timelineData = scope.timelineData;

  //Globals
  let width = element.parent()[0].offsetWidth;
  let height = 50;
  let paddingLeft = 15;
  let paddingRight = 15;
  let maxRange = width - paddingLeft - paddingRight;
  let sliderLimit = maxRange + 24;
  let pinX = 0;
  let sliderX = 0;
  let timelineStack = {};
  let leftVal = 0;

  //Componenets
  let leftHandle;
  let pinHandle;
  let brush;
  let xScale;

  scope.plot = function plot() {
    // -----------------Define SVG and Plot Circles-------------------------- //
    let svg = d3.select('.timeline-log-chart')
                .append('svg')
                .attr('width', width)
                .attr('height', height);
    //Set the Range and Domain
    xScale = d3.time.scale().range([0, (maxRange)]);
    xScale.domain(d3.extent(timelineData, function(d) {
      return d.time;
    }));
    //Define the axes and ticks
    let xAxis = d3.svg.axis().scale(xScale)
        .orient('bottom')
        .innerTickSize(-40)
        .tickPadding(7)
        .ticks(8);

    //Generate circles from the filtered events
    let circles = svg.selectAll('circle')
      .data(timelineData)
      .enter()
      .append('circle');

    circles.attr('cx', function(d) {
      let xVal = Math.floor(xScale(d.time));
      if(timelineStack[xVal] === undefined){
        timelineStack[xVal] = 0;
      } else {
        timelineStack[xVal]++;
      }
      return xScale(d.time) + 15;
    })
    .attr('cy', function(d) {
      let numDots = timelineStack[Math.floor(xScale(d.time))]--;
      return height-height/2.5 - (numDots * 6);
    })
    .attr('r', 2)
    .attr('class', function(d) {
      if(d.level === 'ERROR'){
        return 'red-circle';
      }
      else if(d.level === 'WARN'){
        return 'yellow-circle';
      } else {
        return 'other-circle';
      }
    });

    // -------------------------Build Brush / Sliders------------------------- //
    //X-Axis
    svg.append('g')
      .attr('class', 'xaxis-bottom')
      .attr('transform', 'translate(' + ( (paddingLeft + paddingRight) / 2) + ',' + (height - 20) + ')')
      .call(xAxis);

    //attach handler to brush
    brush = d3.svg.brush()
        .x(xScale)
        .on('brush', function(){
          if(d3.event.sourceEvent) {
            let v = xScale.invert(d3.mouse(this)[0]);
            let index = d3.mouse(this)[0];
            if(v !== leftVal){
              leftVal = v;
            }
            updateSlider(index);
          }
        });

    //Fix me: Make me self reliant (see 'xScale' and other variables)
    function updateSlider(val) {
      //Update the brush position
      if(val < 0){
        val = 0;
      }
      if(val > sliderLimit){
        val = sliderLimit;
      }

      sliderX = val;

      //If the pin is at the top of the table, keep it at the top
      if(sliderX >= pinX){
        updatePin(sliderX);
      }

      leftHandle.attr('x', val);
      sliderBar.attr('d', 'M0,0V0H' + val + 'V0');
      scope.Timeline.updateStartTimeInStore(xScale.invert(val));
    }

    //Make the update event available to the controller
    scope.updateSlider = updateSlider;

    //Creates the top slider and trailing dark background
    let sliderBar = svg.append('g')
      .attr('class', 'slider leftSlider')
      .call(d3.svg.axis()
        .scale(xScale)
        .tickSize(0)
        .tickFormat(''))
      .select('.domain')
      .attr('class', 'fill-bar');

    sliderBar.attr('d', 'M0,0V0H' + xScale(0) + 'V0');

    let slide = svg.append('g')
          .attr('class', 'slider sliderGroup')
          .attr('transform' , 'translate(0,10)')
          .call(brush);

    leftHandle = slide.append('rect')
        .attr('height', 50)
        .attr('width', 7)
        .attr('x', 0)
        .attr('y', -10)
        .attr('class', 'left-handle');

    //Append the Top slider
    let brush2 = d3.svg.brush()
        .x(xScale)
        .on('brush', function(){
          let xPos = d3.mouse(this)[0];

          if(xPos < 0){
            xPos = 0;
          }

          if(xPos > width - 8){
            xPos = width - 8;
          }

          if(xPos > sliderX){
            updatePin(xPos);
          }
        });

    let svg2 = d3.select('.top-bar').append('svg')
        .attr('width', width)
        .attr('height', 20)
      .append('g');

    svg2.append('g')
        .attr('class', 'xaxis-top')
        .call(d3.svg.axis()
          .scale(xScale)
          .orient('bottom'))
      .select('.domain')
      .select(function(){ return this.parentNode.appendChild(this.cloneNode(true));})
        .attr('class', 'halo');

    let slider = svg2.append('g')
        .attr('class', 'slider')
        .attr('width', width)
        .call(brush2);

    slider.select('.background')
      .attr('height', 15);

    pinHandle = slider.append('rect')
        .attr('width', 15)
        .attr('height', 15)
        .attr('x', 0)
        .attr('y', 0)
        .attr('class', 'scroll-pin');

    var updatePin = function (val) {
      pinX = val;
      pinHandle.attr('x', val);
    };

    scope.updatePin = updatePin;
  };

  scope.plot();
}

angular.module(PKG.name + '.commons')
.directive('myTimeline', function() {
  return {
    templateUrl: 'timeline/timeline.html',
    scope: {
      timelineData: '=?'
    },
    link: link,
    controller: 'TimelineController',
    controllerAs: 'Timeline'
  };
});
