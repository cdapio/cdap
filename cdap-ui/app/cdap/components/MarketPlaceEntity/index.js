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
import React, {PropTypes} from 'react';
import classnames from 'classnames';
require('./MarketPlaceEntity.less');

export default function MarketPlaceEntity({size, className, style, name, subtitle, icon, onClick}) {
  return (
    <div
      className={classnames("cask-marketplace-entity-card", className, size)}
      style={style}
    >
      <div
        className="image-container"
        onClick={onClick}
      >
        <img src={icon} />
      </div>
      <div className="metadata-container" onClick={onClick}>
        <div className="metadata-version">{subtitle}</div>
        <div className="metadata-name">{name}</div>
      </div>
    </div>
  );
}

MarketPlaceEntity.defaultProps = {
  size: 'small'
};

MarketPlaceEntity.propTypes = {
  size: PropTypes.string,
  className: PropTypes.string,
  style: PropTypes.object,
  name: PropTypes.string,
  subtitle: PropTypes.string,
  icon: PropTypes.string,
  onClick: PropTypes.func
};
