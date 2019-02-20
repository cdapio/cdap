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

import * as React from 'react';
import T from 'i18n-react';

import './SectionTitle.scss';

const PREFIX = 'features.FieldLevelLineage.SectionTitle';

interface IProps {
  entityId: string;
  parentId?: string;
  direction: 'self' | 'incoming' | 'outgoing';
}

const SectionTitle: React.SFC<IProps> = ({ direction, entityId, parentId }) => {
  let parent;
  if (parentId) {
    parent = <span>{parentId}:</span>;
  }

  return (
    <h5 className="section-title">
      <strong>{T.translate(`${PREFIX}.${direction}`)}:</strong>
      {parent}
      <strong>{entityId}</strong>
    </h5>
  );
};

export default SectionTitle;
