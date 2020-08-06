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

import AbstractMultiRowWidget, {
  IMultiRowProps,
} from 'components/AbstractWidget/AbstractMultiRowWidget';
import DLPRow, {
  FilterOption,
  ITransformProp,
} from 'components/AbstractWidget/DLPCustomWidget/DLPRow';

import ThemeWrapper from 'components/ThemeWrapper';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import { objectQuery } from 'services/helpers';

interface IDLPWidgetProps {
  transforms: ITransformProp[];
  filters: FilterOption[];
  delimiter?: string;
}

interface IDLPProps extends IMultiRowProps<IDLPWidgetProps> {}

class DLPWidgetView extends AbstractMultiRowWidget<IDLPProps> {
  public deconstructValues = (props) => {
    try {
      return JSON.parse(props.value);
    } catch (error) {
      return [];
    }
  };

  public constructValues = () => {
    return JSON.stringify(
      this.state.rows
        .filter((id) => this.values[id] && this.values[id].value)
        .map((id) => this.values[id].value)
    );
  };

  public renderRow = (id, index) => {
    const transforms = objectQuery(this.props, 'widgetProps', 'transforms');
    const filters = objectQuery(this.props, 'widgetProps', 'filters');
    return (
      <DLPRow
        key={id}
        value={this.values[id].value}
        id={id}
        index={index}
        onChange={this.editRow}
        addRow={this.addRow.bind(this, index)}
        removeRow={this.removeRow.bind(this, index)}
        autofocus={this.state.autofocus === id}
        changeFocus={this.changeFocus}
        disabled={this.props.disabled}
        transforms={transforms}
        filters={filters}
        forwardedRef={this.values[id].ref}
        errors={this.props.errors}
        extraConfig={this.props.extraConfig}
      />
    );
  };
}

export default function DLPWidget(props) {
  return (
    <ThemeWrapper>
      <DLPWidgetView {...props} />
    </ThemeWrapper>
  );
}

(DLPWidget as any).propTypes = WIDGET_PROPTYPES;
