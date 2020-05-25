const a = [
  {
    name: 'etlSchemaBody',
    schema: {
      type: 'record',
      name: 'etlSchemaBody',
      fields: [
        {
          name: 'name',
          type: 'string',
        },
        {
          name: 'email',
          type: 'string',
        },
        {
          name: 'phone',
          type: ['string', 'null'],
        },
        {
          name: 'age',
          type: 'string',
        },
        {
          name: 'arr',
          type: {
            type: 'array',
            items: 'string',
          },
        },
        {
          name: 'map1',
          type: {
            type: 'map',
            keys: 'string',
            values: 'string',
          },
        },
        {
          name: 'something',
          type: [
            {
              type: 'map',
              keys: ['string'],
              values: ['int'],
            },
            'string',
          ],
        },
        {
          name: 'enum1',
          type: {
            type: 'enum',
            symbols: ['something', 'somethingelse', 'nothing', 'maybesomething'],
          },
        },
      ],
    },
  },
];

export default a;
