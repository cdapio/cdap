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

import * as React from 'react';
import { connect } from 'react-redux';
import ChooseSource from 'components/DataSync/Create/ChooseSource';
import SourceConfig from 'components/DataSync/Create/SourceConfig';
import ChooseSink from 'components/DataSync/Create/ChooseSink';
import SinkConfig from 'components/DataSync/Create/SinkConfig';
import Summary from 'components/DataSync/Create/Summary';

interface IViewerMapProps {
  activeStep: number;
}

const COMPONENT_MAP = {
  0: ChooseSource,
  1: SourceConfig,
  2: ChooseSink,
  3: SinkConfig,
  5: Summary,
};

function renderComp(step) {
  const Tag = COMPONENT_MAP[step];

  return <Tag />;
}

const ViewerMapView: React.SFC<IViewerMapProps> = ({ activeStep }) => {
  return <div className="datasync-config-body-container">{renderComp(activeStep)}</div>;
};

const mapStateToProps = (state) => {
  return {
    activeStep: state.datasync.activeStep,
  };
};

const ViewerMap = connect(mapStateToProps)(ViewerMapView);

export default ViewerMap;
