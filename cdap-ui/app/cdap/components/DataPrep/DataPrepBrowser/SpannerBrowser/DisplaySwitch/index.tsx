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
import SpannerInstanceList from 'components/DataPrep/DataPrepBrowser/SpannerBrowser/InstanceList';
import SpannerDatabaseList from 'components/DataPrep/DataPrepBrowser/SpannerBrowser/DatabaseList';
import SpannerTableList from 'components/DataPrep/DataPrepBrowser/SpannerBrowser/TableList';
import { connect } from 'react-redux';

interface ISpannerDisplaySwitchProps {
  instanceId: string;
  databaseId: string;
  scope: boolean | string;
  onWorkspaceCreate: () => void;
}

const SpannerDisplaySwitchView: React.SFC<ISpannerDisplaySwitchProps> = (props) => {
  const { instanceId, databaseId, onWorkspaceCreate, scope } = props;

  if (databaseId) {
    return (
      <SpannerTableList enableRouting={false} onWorkspaceCreate={onWorkspaceCreate} scope={scope} />
    );
  } else if (instanceId) {
    return <SpannerDatabaseList enableRouting={false} />;
  }
  return <SpannerInstanceList enableRouting={false} />;
};

const mapStateToProps = (state): Partial<ISpannerDisplaySwitchProps> => {
  return {
    instanceId: state.spanner.instanceId,
    databaseId: state.spanner.databaseId,
  };
};

const SpannerDisplaySwitch = connect(mapStateToProps)(SpannerDisplaySwitchView);

export default SpannerDisplaySwitch;
