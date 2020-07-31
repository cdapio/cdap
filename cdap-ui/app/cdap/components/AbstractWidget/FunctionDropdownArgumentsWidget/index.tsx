/*
 * Copyright © 2020 Cask Data, Inc.
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
import FunctionDropdownArgumentsRow, {
  IDropdownOption,
} from 'components/AbstractWidget/FunctionDropdownArgumentsWidget/FunctionDropdownArgumentsRow';
import ThemeWrapper from 'components/ThemeWrapper';
import AbstractMultiRowWidget, {
  IMultiRowProps,
} from 'components/AbstractWidget/AbstractMultiRowWidget';
import { objectQuery } from 'services/helpers';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';

interface IFunctionDropdownWidgetProps {
  placeholders?: Record<string, string>;
  dropdownOptions: IDropdownOption[];
  delimiter?: string;
}

interface IFunctionDropdownProps extends IMultiRowProps<IFunctionDropdownWidgetProps> {}

class FunctionDropdownArgumentsWidgetView extends AbstractMultiRowWidget<IFunctionDropdownProps> {
  public renderRow = (id, index) => {
    const placeholders = objectQuery(this.props, 'widgetProps', 'placeholders');
    const dropdownOptions = objectQuery(this.props, 'widgetProps', 'dropdownOptions');
    return (
      <FunctionDropdownArgumentsRow
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
        placeholders={placeholders}
        dropdownOptions={dropdownOptions}
        forwardedRef={this.values[id].ref}
        errors={this.props.errors}
      />
    );
  };
}

export default function FunctionDropdownArgumentsWidget(props) {
  return (
    <ThemeWrapper>
      <FunctionDropdownArgumentsWidgetView {...props} />
    </ThemeWrapper>
  );
}

(FunctionDropdownArgumentsWidget as any).propTypes = WIDGET_PROPTYPES;
