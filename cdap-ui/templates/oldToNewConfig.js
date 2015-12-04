var path = [
  'cdap-etl-batch',
  'cdap-etl-realtime',
  'common'
];

var basePath = __dirname + '/';
var fs = require('fs');

path.forEach((folderName) => {
  console.log('folderName: ', basePath + folderName);
  fs.readdir(basePath + folderName, (err, files) => {
    files.forEach( (file) => {
      fs.readFile(basePath + folderName + '/' + file, 'utf-8', (err, content) => {
        if (err) throw err;
        fs.writeFile(basePath + folderName + '/' +file, JSON.stringify(transform(content, basePath, file), null, 2), (err) => {
          console.log('File ' + basePath + folderName + '/new-' +file + ' written successfully');
        });
      });
    });
  });
});


function transform(content, path, fileName) {
  var config;
  try {
    config = JSON.parse(content);
  } catch(e) {
    console.error(e);
    return;
  }
  if (config.metadata) { return; }
  var generatedConfig  = {
    metadata: {
      'spec-version': '1.0',
      'artifact-version': '3.3.0-SNAPSHOT',
      name: config.id || '',
      type: ''
    },
    'configuration-groups': [],
    outputs: []
  };
  var groups = config.groups.position;

  groups.forEach((group) => {
    var modGroup = {
      label: '',
      properties: []
    };
    var oldGroup = config.groups[group];
    var fields = oldGroup.position;
    modGroup.label = oldGroup.display;

    fields.forEach( field => {
      var oldField = oldGroup.fields[field];
      var modField = {};
      modField['widget-type'] = oldField.widget;
      modField['label'] = oldField.label || field;
      modField['name'] = field;

      if (oldField.properties) {
        modField['widget-attributes'] = oldField.properties;
      }
      modGroup.properties.push(modField);
    });
    generatedConfig['configuration-groups'].push(modGroup);
  });

  if (config.outputschema) {
    if (config.outputschema.implicit) {
      generatedConfig.outputs.push({
        'widget-type': 'non-editable-schema-editor',
        'schema': config.outputschema.implicit
      });
    } else {
      var outputProperties = Object.keys(config.outputschema);
      outputProperties.forEach( property => {
        var oldSchema = config.outputschema[property];
        generatedConfig.outputs.push({
          'widget-type': oldSchema.widget,
          'widget-attributes': {
            'schema-types': oldSchema['schema-types'],
            'schema-default-type': oldSchema['schema-default-type'],
            'property-watch': oldSchema['property-watch']
          }
        })
      });
    }
  }

  return generatedConfig;
}
