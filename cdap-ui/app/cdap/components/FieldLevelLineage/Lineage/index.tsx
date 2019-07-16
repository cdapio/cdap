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
import Fields from 'components/FieldLevelLineage/Fields';
import IncomingLineage from 'components/FieldLevelLineage/LineageSummary/IncomingLineage';
import OutgoingLineage from 'components/FieldLevelLineage/LineageSummary/OutgoingLineage';
import OperationsModal from 'components/FieldLevelLineage/OperationsModal';
import './Lineage.scss';

const Lineage: React.SFC = () => {
  return (
    <div className="field-level-lineage-container">
      <div className="row">
        <div className="col-4" data-cy="incoming-fields">
          <IncomingLineage />
        </div>

        <div className="col-4" data-cy="target-fields">
          <Fields />
        </div>

        <div className="col-4" data-cy="impact-fields">
          <OutgoingLineage />
        </div>
      </div>
      <OperationsModal />
    </div>
  );
};

export default Lineage;
