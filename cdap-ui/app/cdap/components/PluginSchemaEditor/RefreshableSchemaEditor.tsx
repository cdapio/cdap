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
import { SchemaEditor } from 'components/AbstractWidget/SchemaEditor';
import If from 'components/If';
import LoadingSVG from 'components/LoadingSVG';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
const styles = (): StyleRules => {
  return {
    container: {
      textAlign: 'center',
    },
  };
};

interface IPluginSchema {
  name: string;
  schema: string;
}

interface IRefreshableSchemaEditor extends WithStyles<typeof styles> {
  schema: IPluginSchema;
  onChange: (schemas: IPluginSchema) => void;
  disabled?: boolean;
  visibleRows?: number;
}

function RefreshableSchemaEditorBase({
  schema,
  onChange,
  disabled,
  classes,
  visibleRows,
}: IRefreshableSchemaEditor) {
  const [loading, setLoading] = React.useState(false);

  React.useEffect(() => {
    setLoading(true);
    setTimeout(() => {
      setLoading(false);
    }, 500);
  }, [schema]);

  return (
    <div className={classes.container}>
      <If condition={loading}>
        <LoadingSVG />
      </If>
      <If condition={!loading}>
        <SchemaEditor
          schema={schema}
          disabled={disabled}
          onChange={onChange}
          visibleRows={visibleRows}
        />
      </If>
    </div>
  );
}
const RefreshableSchemaEditor = withStyles(styles)(RefreshableSchemaEditorBase);
export { RefreshableSchemaEditor };
