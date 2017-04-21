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

import React, { Component, PropTypes } from 'react';
import SchemaStore from 'components/SchemaEditor/SchemaStore';
import SchemaEditor from 'components/SchemaEditor';
import {Tooltip} from 'reactstrap';
import T from 'i18n-react';
require('./SchemaTab.scss');

export default class SchemaTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity,
      tooltipOpen: false
    };

    this.toggleTooltip = this.toggleTooltip.bind(this);
  }

  componentWillMount() {
    if (!this.props.entity.schema) { return; }
    let schema;
    try {
      schema = JSON.parse(this.props.entity.schema);
    } catch (e) {
      console.error('Error parsing schema: ', e);
      schema = { fields: []};
    }
    this.setSchema({fields: schema.fields});
  }

  componentWillUnmount() {
    SchemaStore.dispatch({
      type: 'RESET'
    });
  }

  setSchema(schema) {
    SchemaStore.dispatch({
      type: 'FIELD_UPDATE',
      payload: {
        schema
      }
    });
  }

  toggleTooltip() {
    this.setState({tooltipOpen: !this.state.tooltipOpen});
  }

  render() {
    const infoIconId = 'schema-tab-title-info';

    return (
      <div className="schema-tab">
        <div className="message-section">
          <strong> {T.translate('features.Overview.SchemaTab.title', {entityType: this.state.entity.type, entityId: this.state.entity.id})} </strong>
          <span className="message-section-tooltip">
            <i
              className="fa fa-info-circle"
              id={infoIconId}
            />
            <Tooltip
              isOpen={this.state.tooltipOpen}
              target={infoIconId}
              toggle={this.toggleTooltip}
            >
              {T.translate('features.Overview.SchemaTab.tooltip')}
            </Tooltip>
          </span>
        </div>
        <fieldset
          className="disable-schema"
          disabled
        >
          {
            this.state.entity.schema ?
              <SchemaEditor />
            :
            <div className="empty-schema">
              <i>{T.translate('features.Overview.SchemaTab.emptyMessage')}</i>
            </div>
          }
        </fieldset>
      </div>
    );
  }
}

SchemaTab.propTypes = {
  entity: PropTypes.shape({
    schema: PropTypes.oneOfType([PropTypes.string, PropTypes.object])
  })
};
