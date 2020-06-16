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

import { getCurrentNamespace } from 'services/NamespaceStore';
import { Observable } from 'rxjs/Observable';
import { MyPipelineApi } from 'api/pipeline';
import { MyReplicatorApi } from 'api/replicator';
import { bucketPlugins } from 'services/PluginUtilities';
import { Map } from 'immutable';

export function fetchPluginInfo(
  parentArtifact,
  artifactName,
  artifactScope,
  pluginName,
  pluginType
) {
  const observable$ = Observable.create((observer) => {
    const pluginParams = {
      namespace: getCurrentNamespace(),
      parentArtifact: parentArtifact.name,
      version: parentArtifact.version,
      extension: pluginType,
      pluginName,
      scope: parentArtifact.scope,
      artifactName,
      artifactScope,
      limit: 1,
      order: 'DESC',
    };

    MyPipelineApi.getPluginProperties(pluginParams).subscribe(
      ([res]) => {
        observer.next(res);
        observer.complete();
      },
      (err) => {
        observer.error(err);
      }
    );
  });

  return observable$;
}

export function fetchPluginWidget(
  artifactName,
  artifactVersion,
  artifactScope,
  pluginName,
  pluginType
) {
  const observable$ = Observable.create((observer) => {
    const widgetKey = `widgets.${pluginName}-${pluginType}`;
    const params = {
      namespace: getCurrentNamespace(),
      artifactName,
      artifactVersion,
      scope: artifactScope,
      keys: widgetKey,
    };

    MyPipelineApi.fetchWidgetJson(params).subscribe(
      (res) => {
        if (!res || !res[widgetKey]) {
          observer.next({});
          observer.complete();
          return;
        }

        try {
          const widgetContent = JSON.parse(res[widgetKey]);

          observer.next(widgetContent);

          observer.complete();
        } catch (parseError) {
          observer.error(parseError);
        }
      },
      (err) => {
        observer.error(err);
      }
    );
  });

  return observable$;
}

export function fetchPluginsAndWidgets(parentArtifact, pluginType) {
  const observable$ = Observable.create((observer) => {
    const params = {
      namespace: getCurrentNamespace(),
      parentArtifact: parentArtifact.name,
      version: parentArtifact.version,
      scope: parentArtifact.scope,
      pluginType,
    };

    MyReplicatorApi.getPlugins(params).subscribe(
      (res) => {
        const pluginsBucket = bucketPlugins(res);

        // convert plugins buckets into array with only latest version
        const plugins = Object.keys(pluginsBucket).map((pluginName) => {
          return pluginsBucket[pluginName][0];
        });

        const widgetRequestBody = plugins.map((plugin) => {
          const propertyKey = `${plugin.name}-${plugin.type}`;
          const pluginReqBody = {
            ...plugin.artifact,
            properties: [`widgets.${propertyKey}`],
          };
          return pluginReqBody;
        });

        MyReplicatorApi.batchGetPluginsWidgets(
          { namespace: getCurrentNamespace() },
          widgetRequestBody
        ).subscribe(
          (widgetRes) => {
            const processedWidgetMap = {};

            widgetRes.forEach((plugin) => {
              Object.keys(plugin.properties).forEach((propertyName) => {
                const pluginKey = propertyName.split('.')[1];

                let widget = {};
                try {
                  widget = JSON.parse(plugin.properties[propertyName]);
                } catch (e) {
                  // tslint:disable-next-line: no-console
                  console.log('Failed to parse widget JSON', e);
                }

                processedWidgetMap[pluginKey] = widget;
              });
            });

            observer.next({
              plugins,
              widgetMap: processedWidgetMap,
            });
            observer.complete();
          },
          (err) => {
            observer.error(err);
          }
        );
      },
      (err) => {
        observer.error(err);
      }
    );
  });

  return observable$;
}

export function generateTableKey(row) {
  let database = row.database;
  let table = row.table;
  if (Map.isMap(row)) {
    database = row.get('database');
    table = row.get('table');
  }

  return `db-${database}-table-${table}`;
}

interface IColumn {
  name: string;
  type: string;
}

enum DML {
  insert = 'INSERT',
  update = 'UPDATE',
  delete = 'DELETE',
}

interface ITableObj {
  database: string;
  table: string;
  schema?: string;
  columns?: IColumn[];
  dmlBlacklist?: DML[];
}
export function constructTablesSelection(tables, columns, dmlBlacklist) {
  if (!tables) {
    return [];
  }

  const tablesArr = [];

  /**
   * {
   *    database,
   *    table
   *    schema
   * }
   */

  tables.toList().forEach((row) => {
    const database = row.get('database');
    const table = row.get('table');

    const tableObj: ITableObj = {
      database,
      table,
    };

    const schemaName = row.get('schema');

    if (schemaName) {
      tableObj.schema = schemaName;
    }

    const tableKey = generateTableKey(tableObj);
    const selectedColumns = columns.get(tableKey);

    if (selectedColumns && selectedColumns.size > 0) {
      const tableColumns = [];
      selectedColumns.forEach((column) => {
        const columnObj = {
          name: column.get('name'),
          type: column.get('type'),
        };

        tableColumns.push(columnObj);
      });

      tableObj.columns = tableColumns;
    }

    const tableDML = dmlBlacklist.get(tableKey);
    if (tableDML && tableDML.size > 0) {
      tableObj.dmlBlacklist = tableDML.toArray();
    }

    tablesArr.push(tableObj);
  });

  return tablesArr;
}
