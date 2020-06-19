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

import React, { memo, useMemo, useState, useEffect } from 'react';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import { useScroll } from 'components/VirtualScroll/useScroll';

interface IVirtualScrollProps extends WithStyles<typeof styles> {
  renderList: (
    visibleNodeCount: number,
    startNode: number
  ) => React.ReactNode | Promise<React.ReactNode>;
  itemCount: number | (() => number);
  visibleChildCount: number;
  childHeight: number;
  childrenUnderFold: number;
  LoadingElement?: React.ReactNode;
}
const styles = (): StyleRules => {
  return {
    root: {
      overflow: 'auto',
    },
    viewport: {
      paddingTop: '1px',
      overflow: 'hidden',
      willChange: 'transform',
      position: 'relative',
    },
    loading: {
      position: 'absolute',
      background: 'purple',
      bottom: '0',
      height: '30px',
      color: 'white',
      width: '200px',
      marginLeft: '10px',
      textAlign: 'center',
    },
  };
};

// VirtualScroll component
const VirtualScroll = ({
  renderList,
  itemCount,
  visibleChildCount,
  childHeight,
  childrenUnderFold,
  classes,
  LoadingElement = () => 'Loading...',
}: IVirtualScrollProps) => {
  const [scrollTop, ref] = useScroll();
  const itmCount = typeof itemCount === 'function' ? itemCount() : itemCount;
  const totalHeight = itmCount * childHeight;
  const [list, setList] = useState<React.ReactNode>([]);
  const [promise, setPromise] = useState(null);

  let startNode = Math.floor(scrollTop / childHeight) - childrenUnderFold;
  startNode = Math.max(0, startNode);

  const visibleNodeCount = visibleChildCount + 2 * childrenUnderFold;

  const offsetY = startNode * childHeight;

  useMemo(
    () => {
      const newList = renderList(visibleNodeCount, startNode);
      if (Array.isArray(newList)) {
        setList(newList);
      }
      if (Array.isArray(newList) && newList.length < visibleNodeCount) {
        const p = renderList(visibleChildCount, startNode);
        if (p instanceof Promise && Array.isArray(list)) {
          setPromise(p);
        }
      }
    },
    [startNode, visibleNodeCount, renderList]
  );

  useEffect(
    () => {
      if (!promise) {
        return;
      }
      promise.then((newList) => {
        setList(newList);
        setPromise(null);
      });
    },
    [promise]
  );

  const containerHeight =
    itmCount > visibleChildCount ? visibleChildCount * childHeight : itmCount * childHeight;
  return (
    <div style={{ height: containerHeight }} className={classes.root} ref={ref}>
      <div
        style={{
          height: totalHeight,
        }}
        className={classes.viewport}
      >
        <div
          style={{
            willChange: 'transform',
            transform: `translateY(${offsetY}px)`,
          }}
        >
          {list}
        </div>
        {promise ? <div className={classes.loading}>Loading...</div> : null}
      </div>
    </div>
  );
};

export default memo(withStyles(styles)(VirtualScroll));
