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

import PropTypes from 'prop-types';
import React from 'react';
import classnames from 'classnames';

export default function NodeMetricsGraphLegend({
  item,
  orientation,
  showCheckbox,
  onLegendClick,
  itemChecked,
}) {
  if (!showCheckbox) {
    return (
      <div className={classnames('rv-discrete-color-legend-item', orientation)}>
        <span className="rv-discrete-color-legend-item__color" style={{ background: item.color }} />
        <span className="rv-discrete-color-legend-item__title">{item.title}</span>
      </div>
    );
  }

  return (
    <div
      className={classnames('rv-discrete-color-legend-item pointer', orientation)}
      onClick={onLegendClick.bind(null, item.title)}
    >
      <span
        className={classnames('fa legend-item-checkbox', {
          'fa-square-o': !itemChecked,
          'fa-check-square': itemChecked,
        })}
      />
      <span className="rv-discrete-color-legend-item__color" style={{ background: item.color }} />
      <span className="rv-discrete-color-legend-item__title">{item.title}</span>
    </div>
  );
}

NodeMetricsGraphLegend.propTypes = {
  item: PropTypes.oneOfType([
    PropTypes.shape({
      title: PropTypes.string.isRequired,
      color: PropTypes.string,
      disabled: PropTypes.bool,
    }),
    PropTypes.string.isRequired,
    PropTypes.element,
  ]).isRequired,
  orientation: PropTypes.string,
  showCheckbox: PropTypes.bool,
  onLegendClick: PropTypes.func,
  itemChecked: PropTypes.bool,
};

NodeMetricsGraphLegend.defaultProps = {
  orientation: 'horizontal',
  showCheckbox: false,
};
