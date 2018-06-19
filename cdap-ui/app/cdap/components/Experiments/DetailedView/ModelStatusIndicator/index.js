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
import {MODEL_STATUS} from 'components/Experiments/store/ModelStatus';
import {MODEL_STATUS_TO_COLOR_MAP} from 'components/Experiments/DetailedView/ExperimentMetricsDropdown/ModelStatusesDistribution';

require('./ModelStatusIndicator.scss');

const DEFAULT_STATUS_MAP = {
  className: 'text-info',
  icon: 'icon-circle-o'
};

const STATUS_ICON_MAP = {
  [MODEL_STATUS.SPLITTING]: {
    className: 'fa-spin ',
    icon: 'icon-spinner',
    color: MODEL_STATUS_TO_COLOR_MAP[MODEL_STATUS.SPLITTING]
  },
  [MODEL_STATUS.SPLIT_FAILED]: {
    className: '',
    icon: 'icon-circle-o',
    color: MODEL_STATUS_TO_COLOR_MAP[MODEL_STATUS.SPLIT_FAILED]
  },
  [MODEL_STATUS.DATA_READY]: {
    className: '',
    icon: 'icon-circle-o',
    color: MODEL_STATUS_TO_COLOR_MAP[MODEL_STATUS.DATA_READY]
  },
  [MODEL_STATUS.TRAINING]: {
    className: 'fa-spin',
    icon: 'icon-spinner',
    color: MODEL_STATUS_TO_COLOR_MAP[MODEL_STATUS.TRAINING]
  },
  [MODEL_STATUS.TRAINED]: {
    className: '',
    icon: 'icon-circle-o',
    color: MODEL_STATUS_TO_COLOR_MAP[MODEL_STATUS.TRAINED]
  },
  [MODEL_STATUS.TRAINING_FAILED]: {
    className: '',
    icon: 'icon-circle-o',
    color: MODEL_STATUS_TO_COLOR_MAP[MODEL_STATUS.TRAINING_FAILED]
  },
  [MODEL_STATUS.PREPARING]: {
    className: 'fa-spin',
    icon: 'icon-spinner',
    color: MODEL_STATUS_TO_COLOR_MAP[MODEL_STATUS.PREPARING]
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
          {`Failed to get the status of the model '${model.name}'. Click to try loading the status again`}
        </UncontrolledTooltip>
      </span>
    );
  }

  let iconMap = getIconMap(status);
  return (
    <span className="model-status-indicator" title={status}>
      <IconSVG
        name={iconMap.icon}
        className={iconMap.className}
        style={{
          color: iconMap.color
        }}
      />
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
