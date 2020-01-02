/*
 * Copyright © 2019 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import { ACTIONS as PipelineConfigurationsActions } from 'components/PipelineConfigurations/Store';
import T from 'i18n-react';

const PREFIX = 'features.PipelineConfigurations.PipelineConfig';

const mapStateToServiceAccountPathProps = (state) => {
  return {
    serviceAccountPath: state.serviceAccountPath,
  };
};

const mapDispatchToServiceAccountPathProps = (dispatch) => {
  return {
    onServiceAccountPathChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_SERVICE_ACCOUNT_PATH,
        payload: { serviceAccountPath: e.target.value },
      });
    },
  };
};

const ServiceAccountPath = ({ serviceAccountPath, onServiceAccountPathChange }) => {
  return (
    <React.Fragment>
      <div className="label-with-toggle row">
        <span className="toggle-label col-4">{T.translate(`${PREFIX}.serviceAccountPath`)}</span>
        <div className="col-7">
          <input
            type="text"
            className="form-control"
            value={serviceAccountPath}
            onChange={onServiceAccountPathChange}
          />
          <Popover
            target={() => <IconSVG name="icon-info-circle" />}
            showOn="Hover"
            placement="right"
          >
            {T.translate(`${PREFIX}.serviceAccountPathTooltip`)}
          </Popover>
        </div>
      </div>
    </React.Fragment>
  );
};

ServiceAccountPath.propTypes = {
  onServiceAccountPathChange: PropTypes.func,
  serviceAccountPath: PropTypes.string,
};

const ConnectedServiceAccountPath = connect(
  mapStateToServiceAccountPathProps,
  mapDispatchToServiceAccountPathProps
)(ServiceAccountPath);

export default ConnectedServiceAccountPath;
