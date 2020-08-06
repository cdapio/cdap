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
import FunctionDropdownRow, {
  IDropdownOption,
} from 'components/AbstractWidget/FunctionDropdownAliasWidget/FuctionDropdownRow';

import ThemeWrapper from 'components/ThemeWrapper';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import { objectQuery } from 'services/helpers';

interface IFunctionDropdownWidgetProps {
  placeholders?: Record<string, string>;
  dropdownOptions: IDropdownOption[];
  delimiter?: string;
}

interface IFunctionDropdownProps extends IMultiRowProps<IFunctionDropdownWidgetProps> {}

class FunctionDropdownAliasWidgetView extends AbstractMultiRowWidget<IFunctionDropdownProps> {
  public renderRow = (id, index) => {
    const placeholders = objectQuery(this.props, 'widgetProps', 'placeholders');
    const dropdownOptions = objectQuery(this.props, 'widgetProps', 'dropdownOptions');
    return (
      <FunctionDropdownRow
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

export default function FunctionDropdownAliasWidget(props) {
  return (
    <ThemeWrapper>
      <FunctionDropdownAliasWidgetView {...props} />
    </ThemeWrapper>
  );
}

(FunctionDropdownAliasWidget as any).propTypes = WIDGET_PROPTYPES;
(FunctionDropdownAliasWidget as any).getWidgetAttributes = () => {
  return {
    placeholders: { type: 'Record<string, string>', required: false },
    dropdownOptions: { type: 'IDropdownOption[]', required: true },
    delimiter: { type: 'string', required: false },
  };
};
