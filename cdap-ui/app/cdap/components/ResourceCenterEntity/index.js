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

require('./ResourceCenterEntity.less');

export default function ResourceCenterEntity({className, img, title, description, actionLabel, onClick}) {
  return (
    <div className={classnames('resourcecenter-entity-card', className)}>
      {
        img ?
          <img className="entity-image" src={img} /> :
          <div className="entity-image"></div>
      }
      <div className="content-container">
        <h4>{title}</h4>
        <p>{description}</p>
        <div
          className="action-label"
          onClick={onClick}
        >
          {actionLabel}
        </div>
      </div>
    </div>
  );
}
ResourceCenterEntity.propTypes = {
  img: PropTypes.string,
  title: PropTypes.string,
  description: PropTypes.string,
  actionLabel: PropTypes.string,
  onClick: PropTypes.func.isRequired,
  className: PropTypes.string
};
