/*
 * Copyright © 2021 Cask Data, Inc.
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

import * as React from 'react';
import VirtualScroll from 'components/VirtualScroll';

const ListboxComponent = React.forwardRef<HTMLDivElement>((props, ref) => {
  const { children } = props;
  const itemData = React.Children.toArray(children);
  const itemCount = itemData.length;

  const renderList = (visibleNodeCount, startNode) => {
    return itemData.slice(startNode, startNode + visibleNodeCount).map((option) => {
      return option;
    });
  };

  return (
    <div ref={ref} {...props}>
      <VirtualScroll
        itemCount={itemCount}
        visibleChildCount={7}
        childHeight={48}
        renderList={renderList}
        childrenUnderFold={5}
      />
    </div>
  );
});

export default ListboxComponent;
