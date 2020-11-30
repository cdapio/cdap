/*
 * Copyright © 2020 Cask Data, Inc.
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

import React, { useEffect, useState } from 'react';
import { Observable } from 'rxjs/Observable';
import { timeFormat } from 'd3';

interface IChartContainerProps {
  containerId: string;
  data: any;
  chartRenderer: (id: string, data: any, width?: number, height?: number) => void;
  width?: number;
  height?: number;
  watchHeight?: boolean;
  watchWidth?: boolean;
  className?: string;
}

const ChartContainer: React.FC<IChartContainerProps> = ({
  containerId,
  data,
  chartRenderer,
  width,
  height,
  watchHeight,
  watchWidth,
  className,
}) => {
  const [containerWidth, setContainerWidth] = useState(0);
  const [containerHeight, setContainerHeight] = useState(0);

  function getContainerDimention() {
    const container = document.getElementById(containerId);
    const containerBoundingBox = container.getBoundingClientRect();

    if (watchHeight) {
      setContainerHeight(containerBoundingBox.height);
    }

    if (watchWidth) {
      setContainerWidth(containerBoundingBox.width);
    }
  }

  useEffect(() => {
    // if width and height are defined, don't watch for window resize
    if (width && height) {
      return;
    }

    getContainerDimention();
    const windowResize$ = Observable.fromEvent(window, 'resize')
      .debounceTime(500)
      .subscribe(getContainerDimention);
    return () => {
      windowResize$.unsubscribe();
    };
  }, []);

  useEffect(() => {
    const selectedWidth = width ? width : containerWidth;
    const selectedHeight = height ? height : containerHeight;

    chartRenderer(containerId, data, selectedWidth, selectedHeight);
  }, [data, height, width, containerWidth, containerHeight]);

  return (
    <div className={className} id={containerId}>
      <svg />
    </div>
  );
};

export default ChartContainer;
export const timeFormatMonthDateTime = timeFormat('%m/%d %I:%M %p');
