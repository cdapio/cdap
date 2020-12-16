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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { Observable } from 'rxjs/Observable';
import { timeFormat } from 'd3';

const styles = (): StyleRules => {
  return {
    root: {
      posisition: 'absolute',
    },
  };
};

interface ITooltip {
  top: number;
  left: number;
  isOpen: boolean;
  activeData?: any;
}

export interface ITooltipProps {
  tooltip: ITooltip;
  classes: any;
}

interface IChartContainerProps extends WithStyles<typeof styles> {
  containerId: string;
  data: any;
  chartRenderer: (
    id: string,
    data: any,
    width?: number,
    height?: number,
    onHover?: (top, left, isOpen, activeData) => void,
    onClick?: (top, left, isOpen, activeData) => void
  ) => void;
  width?: number;
  height?: number;
  watchHeight?: boolean;
  watchWidth?: boolean;
  className?: string;
  renderTooltip?: (tooltip) => React.ReactNode;
  onHover?: (data) => void;
  onClick?: (data) => void;
  additionalWatchProperty?: any[];
}

const ChartContainerView: React.FC<IChartContainerProps> = ({
  containerId,
  data,
  chartRenderer,
  width,
  height,
  watchHeight,
  watchWidth,
  className,
  classes,
  renderTooltip,
  onHover,
  onClick,
  additionalWatchProperty,
}) => {
  const [containerWidth, setContainerWidth] = useState(0);
  const [containerHeight, setContainerHeight] = useState(0);
  const [tooltip, setTooltip] = useState<ITooltip>({
    top: 0,
    left: 0,
    isOpen: false,
    activeData: null,
  });

  function onHoverState(top, left, isOpen, d) {
    setTooltip({
      top,
      left,
      isOpen,
      activeData: d || {},
    });
    if (typeof onHover === 'function') {
      onHover(d);
    }
  }

  function onClickState(top, left, isOpen, d) {
    setTooltip({
      top,
      left,
      isOpen,
      activeData: d || {},
    });
    if (typeof onClick === 'function') {
      onClick(d);
    }
  }

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

  let toWatch = [data, height, width, containerWidth, containerHeight];
  if (Array.isArray(additionalWatchProperty)) {
    toWatch = toWatch.concat(additionalWatchProperty);
  }

  useEffect(() => {
    const selectedWidth = width ? width : containerWidth;
    const selectedHeight = height ? height : containerHeight;

    chartRenderer(containerId, data, selectedWidth, selectedHeight, onHoverState, onClickState);
  }, toWatch);

  return (
    <React.Fragment>
      <div className={`${className} ${classes.root}`} id={containerId}>
        <svg />
      </div>
      {typeof renderTooltip === 'function' ? renderTooltip(tooltip) : null}
    </React.Fragment>
  );
};

const ChartContainer = withStyles(styles)(ChartContainerView);
export default ChartContainer;
export const timeFormatMonthDate = timeFormat('%m/%d');
export const timeFormatHourMinute = timeFormat('%I:%M %p');
