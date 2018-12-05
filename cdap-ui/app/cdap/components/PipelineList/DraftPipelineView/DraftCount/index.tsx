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
import T from 'i18n-react';
import { IDraft } from 'components/PipelineList/DraftPipelineView/types';

interface IProps {
  drafts: IDraft[];
}

const PREFIX = 'features.PipelineList.DraftPipelineView';

const DraftCountView: React.SFC<IProps> = ({ drafts }) => {
  return (
    <div className="draft-count">
      {T.translate(`${PREFIX}.draftCount`, { context: drafts.length })}
    </div>
  );
};

const mapStateToProps = (state) => {
  return {
    drafts: state.drafts.list,
  };
};

const DraftCount = connect(mapStateToProps)(DraftCountView);

export default DraftCount;
