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
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import Button from '@material-ui/core/Button';
import { IAttributesComponentProps } from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import { useAttributePopoverStyles } from 'components/AbstractWidget/SchemaEditor/RowButtons/FieldAttributes/FieldAttributesPopoverButton';

function RecordEnumTypeAttributes({
  typeProperties,
  onChange,
  handleClose,
}: IAttributesComponentProps) {
  const [doc, setDoc] = React.useState(typeProperties.doc || '');
  const [aliases, setAlias] = React.useState(typeProperties.aliases || []);
  const classes = useAttributePopoverStyles();

  const onChangeHandler = () => {
    onChange('typeProperties', {
      doc,
      aliases,
    });
    handleClose();
  };
  return (
    <React.Fragment>
      <div className={classes.root}>
        <WidgetWrapper
          pluginProperty={{
            name: 'doc',
            macroSupported: false,
            description: 'documentation for the record',
          }}
          widgetProperty={{
            'widget-type': 'textbox',
            label: 'Doc',
          }}
          value={doc}
          onChange={setDoc}
        />
        <WidgetWrapper
          pluginProperty={{
            name: 'Aliases',
            macroSupported: false,
            description: 'Aliases for the record',
          }}
          widgetProperty={{
            'widget-type': 'csv',
            label: 'Aliases',
          }}
          value={aliases.join(',')}
          onChange={(value) => {
            setAlias(value.split(','));
          }}
        />
      </div>
      <Button variant="contained" color="primary" onClick={onChangeHandler}>
        Save
      </Button>
    </React.Fragment>
  );
}

export { RecordEnumTypeAttributes };
