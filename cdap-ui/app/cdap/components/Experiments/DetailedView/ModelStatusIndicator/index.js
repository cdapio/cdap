/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import IconSVG from 'components/IconSVG';
import capitalize from 'lodash/capitalize';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import {preventPropagation} from 'services/helpers';


require('./ModelStatusIndicator.scss');

const DEFAULT_STATUS_MAP = {
  className: 'text-info',
  icon: 'icon-circle-o'
};

const STATUS_ICON_MAP = {
  TRAINING: {
    className: 'spin fa-spin',
    icon: 'icon-spinner'
  },
  SPLITTING: {
    className: 'spin fa-spin',
    icon: 'icon-spinner'
  },
  TRAINED: {
    className: 'text-success',
    icon: 'icon-circle-o'
  },
  TRAINING_FAILED: {
    className: 'text-danger',
    icon: 'icon-circle-o'
  }
};
const getIconMap = (status) => status in STATUS_ICON_MAP ? STATUS_ICON_MAP[status] : DEFAULT_STATUS_MAP;

export default function ModelStatusIndicator({status, loading, error, model, getModelStatus}) {
  if (loading) {
    return <IconSVG name="icon-spinner" className="fa-spin" />;
  }

  if (error) {
    return (
      <span>
        <span
          className="model-status-error model-status-indicator text-danger"
          id={`error-${model.id}`}
          onClick={(e) => {
            preventPropagation(e);
            getModelStatus();
          }}
        >
          <IconSVG
            className="text-danger"
            name="icon-exclamation-circle"
          />
          <span>Error</span>
        </span>
        <UncontrolledTooltip
          placement="right"
          delay={0}
          target={`error-${model.id}`}
        >
          {`Failed to get status of model '${model.name}'. Click to try fetching model status again`}
        </UncontrolledTooltip>
      </span>
    );
  }

  let iconMap = getIconMap(status);
  return (
    <span className="model-status-indicator" title={status}>
      <IconSVG name={iconMap.icon} className={iconMap.className} />
      <span>{capitalize(status)}</span>
    </span>
  );
}

ModelStatusIndicator.propTypes = {
  status: PropTypes.string.isRequired,
  loading: PropTypes.bool,
  error: PropTypes.bool,
  model: PropTypes.object,
  getModelStatus: PropTypes.func
};
