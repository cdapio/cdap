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
import {connect} from 'react-redux';
import PropTypes from 'prop-types';
import T from 'i18n-react';
require('./EntityCounts.scss');

const PREFIX = 'features.NamespaceDetails.entityCounts';

const mapStateToProps = (state) => {
  return {
    customAppCount: state.customAppCount,
    pipelineCount: state.pipelineCount,
    datasetCount: state.datasetCount
  };
};

const NamespaceDetailsEntityCounts = ({customAppCount, pipelineCount, datasetCount}) => {
  return (
    <div className="namespace-details-entity-count">
      <div className="entity-count">
        <span>{T.translate(`${PREFIX}.customApps`)}</span>
        <div>{customAppCount}</div>
      </div>
      <div className="entity-count">
        <span>{T.translate('commons.pipelines')}</span>
        <div>{pipelineCount}</div>
      </div>
      <div className="entity-count">
        <span>{T.translate('commons.entity.dataset.plural')}</span>
        <div>{datasetCount}</div>
      </div>
    </div>
  );
};

NamespaceDetailsEntityCounts.propTypes = {
  customAppCount: PropTypes.number,
  pipelineCount: PropTypes.number,
  datasetCount: PropTypes.number
};

const ConnectedNamespaceDetailsEntityCounts = connect(mapStateToProps)(NamespaceDetailsEntityCounts);
export default ConnectedNamespaceDetailsEntityCounts;
