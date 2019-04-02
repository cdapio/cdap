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
import DraftTableRow from 'components/PipelineList/DraftPipelineView/DraftTable/DraftTableRow';
import { connect } from 'react-redux';
import { IDraft } from 'components/PipelineList/DraftPipelineView/types';
import EmptyList, { VIEW_TYPES } from 'components/PipelineList/EmptyList';
import SortableHeader from 'components/PipelineList/DraftPipelineView/DraftTable/SortableHeader';

interface IProps {
  drafts: IDraft[];
  currentPage: number;
  pageLimit: number;
}

require('./DraftTable.scss');

const DraftTableView: React.SFC<IProps> = ({ drafts, currentPage, pageLimit }) => {
  function renderBody() {
    if (drafts.length === 0) {
      return <EmptyList type={VIEW_TYPES.draft} />;
    }

    const startIndex = (currentPage - 1) * pageLimit;
    const endIndex = startIndex + pageLimit;
    const filteredDrafts = drafts.slice(startIndex, endIndex);

    return (
      <div className="grid-body">
        {filteredDrafts.map((draft) => {
          return <DraftTableRow draft={draft} key={draft.__ui__.draftId} />;
        })}
      </div>
    );
  }

  return (
    <div className="draft-table grid-wrapper">
      <div className="grid grid-container">
        <div className="grid-header">
          <div className="grid-row">
            <SortableHeader columnName="name" />
            <SortableHeader columnName="type" />
            <SortableHeader columnName="lastSaved" />
            <strong />
          </div>
        </div>

        {renderBody()}
      </div>
    </div>
  );
};

const mapStateToProps = (state) => {
  return {
    drafts: state.drafts.list,
    currentPage: state.drafts.currentPage,
    pageLimit: state.drafts.pageLimit,
  };
};

const DraftTable = connect(mapStateToProps)(DraftTableView);

export default DraftTable;
