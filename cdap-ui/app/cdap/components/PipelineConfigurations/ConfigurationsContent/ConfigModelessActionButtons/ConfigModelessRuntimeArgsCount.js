/*
 * Copyright © 2018 Cask Data, Inc.
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
import {connect} from 'react-redux';
import T from 'i18n-react';
import {TAB_OPTIONS} from 'components/PipelineConfigurations/Store';
import {convertKeyValuePairsObjToMap} from 'components/KeyValuePairs/KeyValueStoreActions';
import isEmpty from 'lodash/isEmpty';
import {getFilteredRuntimeArgs} from 'components/PipelineConfigurations/Store/ActionCreator';

const PREFIX = 'features.PipelineConfigurations.ActionButtons';

const mapStateToProps = (state, ownProps) => {
  return {
    runtimeArgs: state.runtimeArgs,
    activeTab: ownProps.activeTab,
    isHistoricalRun: ownProps.isHistoricalRun
  };
};

const ConfigModelessRuntimeArgsCount = ({runtimeArgs, activeTab, isHistoricalRun}) => {
  let runtimeArgsObj = convertKeyValuePairsObjToMap(runtimeArgs);
  if (activeTab !== TAB_OPTIONS.RUNTIME_ARGS || (isHistoricalRun && isEmpty(runtimeArgsObj))) {
    return null;
  }

  return (
    <span className="num-runtime-args">
      {T.translate(`${PREFIX}.runtimeArgsCount`, {context: getFilteredRuntimeArgs().pairs.length})}
    </span>
  );
};

ConfigModelessRuntimeArgsCount.propTypes = {
  runtimeArgs: PropTypes.object,
  activeTab: PropTypes.string,
  isHistoricalRun: PropTypes.bool
};

const ConnectedConfigModelessRuntimeArgsCount = connect(mapStateToProps)(ConfigModelessRuntimeArgsCount);
export default ConnectedConfigModelessRuntimeArgsCount;
