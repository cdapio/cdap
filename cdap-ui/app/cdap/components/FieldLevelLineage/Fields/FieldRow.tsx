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
import { getLineageSummary } from 'components/FieldLevelLineage/store/ActionCreator';
import classnames from 'classnames';
import { connect } from 'react-redux';
import If from 'components/If';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage';

interface IField {
  name: string;
  lineage: boolean;
}

interface IFieldRowProps {
  field: IField;
  activeField: string;
}

interface IFieldRowState {
  isHovered: boolean;
}

class FieldRowView extends React.PureComponent<IFieldRowProps, IFieldRowState> {
  public state = {
    isHovered: false,
  };

  private onClickHandler = (field) => {
    if (!this.props.field.lineage) {
      return;
    }

    getLineageSummary(this.props.field.name);
  };

  private onMouseEnter = () => {
    if (this.props.field.lineage) {
      return;
    }
    this.setState({
      isHovered: true,
    });
  };

  private onMouseLeave = () => {
    if (this.props.field.lineage) {
      return;
    }
    this.setState({
      isHovered: false,
    });
  };

  public render() {
    const field = this.props.field;
    const isActive = field.name === this.props.activeField;
    const isDisabled = !field.lineage;

    return (
      <div
        className={classnames('field-row truncate', {
          active: isActive,
          disabled: isDisabled,
        })}
        onClick={this.onClickHandler}
        title={field.name}
        onMouseEnter={this.onMouseEnter}
        onMouseLeave={this.onMouseLeave}
      >
        {field.name}

        <If condition={isDisabled && this.state.isHovered}>
          <em className="no-lineage-text">{T.translate(`${PREFIX}.noLineage`)}</em>
        </If>
      </div>
    );
  }
}

const mapStateToProps = (state, ownProp) => {
  return {
    field: ownProp.field,
    activeField: state.lineage.activeField,
  };
};

const FieldRow = connect(mapStateToProps)(FieldRowView);

export default FieldRow;
