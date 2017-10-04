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
import PropTypes from 'prop-types';

import React from 'react';
import PlusButtonStore from 'services/PlusButtonStore';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';

require('./ResourceCenterEntity.scss');

export default function ResourceCenterEntity({className, iconClassName, title, description, actionLabel, onClick, disabled, actionLink}) {
  const closeResourceCenter = () => {
    PlusButtonStore.dispatch({
      type: 'TOGGLE_PLUSBUTTON_MODAL',
      payload: {
        modalState: false
      }
    });
  };
  return (
    <div className={classnames('resourcecenter-entity-card', className)}>
      <div className="image-button-container">
        {
          iconClassName ?
            (
              <div className="entity-image">
                <IconSVG name={iconClassName} />
              </div>
            )
          :
            <div className="entity-image empty" />
        }
        {
          actionLink ?
            (
              <a href={actionLink}>
                <button
                  className={classnames("btn btn-primary")}
                  onClick={closeResourceCenter}
                  disabled={disabled}
                >
                  {actionLabel}
                </button>
              </a>
            )
          :
            (
              <button
                id={(actionLabel + "-" + title).toLowerCase()}
                className={classnames("btn btn-primary")}
                onClick={onClick}
                disabled={disabled}
              >
                {actionLabel}
              </button>
            )
        }
      </div>
      <div className="content-container">
        <div className="content-text">
          <h4>{title}</h4>
          <p>{description}</p>
        </div>
      </div>
    </div>
  );
}
ResourceCenterEntity.propTypes = {
  iconClassName: PropTypes.string,
  title: PropTypes.string,
  description: PropTypes.string,
  actionLabel: PropTypes.string,
  onClick: PropTypes.func.isRequired,
  className: PropTypes.string,
  disabled: PropTypes.bool,
  actionLink: PropTypes.string
};
