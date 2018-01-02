/*
 * Copyright © 2017 Cask Data, Inc.
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
import React, { Component } from 'react';
import T from 'i18n-react';
import IconSVG from 'components/IconSVG';
import NamespaceStore from 'services/NamespaceStore';
import moment from 'moment';

const PREFIX = 'features.PipelineList';

export default class DraftTableRow extends Component {
  static propTypes = {
    draft: PropTypes.object
  };

  constructor(props) {
    super(props);
  }

  getCreationTime(draft) {
    if (!draft.__ui__.creationTime) { return '--'; }

    const format = 'MM-DD-YYYY';

    return moment(draft.__ui__.creationTime).format(format);
  }

  render() {
    let draft = this.props.draft;
    let namespace = NamespaceStore.getState().selectedNamespace;

    let creationTime = this.getCreationTime(draft);

    let link = window.getHydratorUrl({
      stateName: 'hydrator.create',
      stateParams: {
        namespace,
        draftId: draft.__ui__.draftId
      }
    });

    return (
      <div className="table-row">
        <div className="table-column name">
          <a href={link}>
            {draft.name}
          </a>
        </div>
        <div className="table-column type">
          {T.translate(`${PREFIX}.${draft.artifact.name}`)}
        </div>
        <div className="table-column creation-time">
          {creationTime}
        </div>
        <div className="table-column action">
          <IconSVG name="icon-cog" />
        </div>
      </div>
    );
  }
}
