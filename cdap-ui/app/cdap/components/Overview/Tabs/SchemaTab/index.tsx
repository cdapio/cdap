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

import React, { Component } from 'react';
import { Tooltip } from 'reactstrap';
import T from 'i18n-react';
import { SchemaEditor, heightOfRow } from 'components/AbstractWidget/SchemaEditor';
import { ISchemaType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { getDefaultEmptyAvroSchema } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
require('./SchemaTab.scss');

interface IProgram {
  app: string;
  application: string;
  entity: string;
  name: string;
  program: string;
  namespace: string;
  type: string;
  uniqueId: string;
  version: string;
}

interface IEntity {
  app: string;
  id: string;
  name: string;
  programs: IProgram[];
  properties: Record<string, string>;
  type: string;
  schema: string;
}

interface ISchemaTabProps {
  entity: IEntity;
}

interface ISchemaTabState {
  entity: IEntity;
  tooltipOpen: boolean;
  schema: ISchemaType;
  schemaRowCount: number;
}

export default class SchemaTab extends Component<ISchemaTabProps, ISchemaTabState> {
  constructor(props) {
    super(props);
    window.addEventListener('resize', this.calculateSchemaRowCount);
  }

  public containerRef = null;

  public state = {
    entity: this.props.entity,
    tooltipOpen: false,
    schema: null,
    schemaRowCount: null,
  };

  public componentDidMount() {
    if (!this.props.entity.schema) {
      return;
    }
    let schema;
    try {
      schema = JSON.parse(this.props.entity.schema);
    } catch (e) {
      schema = { ...getDefaultEmptyAvroSchema() };
    }
    this.setState({ schema: { name: 'etlSchemaBody', schema } });
  }

  public componentWillUnmount() {
    window.removeEventListener('resize', this.calculateSchemaRowCount);
  }

  public calculateSchemaRowCount = () => {
    if (this.containerRef) {
      const { height } = this.containerRef.getBoundingClientRect();
      let schemaRowCount = Math.floor(height / heightOfRow) - 1;
      if (schemaRowCount < 5) {
        schemaRowCount = 20;
      }
      this.setState({ schemaRowCount });
    }
  };

  public toggleTooltip = () => {
    this.setState({ tooltipOpen: !this.state.tooltipOpen });
  };

  public render() {
    const infoIconId = 'schema-tab-title-info';

    return (
      <div className="schema-tab" ref={(ref) => (this.containerRef = ref)}>
        <div className="message-section">
          <strong>
            {' '}
            {T.translate('features.Overview.SchemaTab.title', {
              entityType: this.state.entity.type,
              entityId: this.state.entity.id,
            })}{' '}
          </strong>
          <span className="message-section-tooltip">
            <i className="fa fa-info-circle" id={infoIconId} />
            <Tooltip
              isOpen={this.state.tooltipOpen}
              target={infoIconId}
              toggle={this.toggleTooltip}
              placement="bottom"
            >
              {T.translate('features.Overview.SchemaTab.tooltip')}
            </Tooltip>
          </span>
        </div>
        <fieldset className="disable-schema" disabled>
          {this.state.schema ? (
            <SchemaEditor
              visibleRows={this.state.schemaRowCount}
              schema={this.state.schema}
              disabled={true}
            />
          ) : (
            <div className="empty-schema">
              <i>{T.translate('features.Overview.SchemaTab.emptyMessage')}</i>
            </div>
          )}
        </fieldset>
      </div>
    );
  }
}
