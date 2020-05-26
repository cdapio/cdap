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
          type: ['string', 'null'],
        },
        {
          name: 'arr',
          type: {
            type: 'array',
            items: ['string', 'null'],
          },
        },
        {
          name: 'map1',
          type: [
            {
              type: 'map',
              keys: ['long'],
              values: ['double'],
            },
            'null',
          ],
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
          type: [
            'null',
            {
              type: 'enum',
              symbols: ['something', 'somethingelse', 'nothing', 'maybesomething'],
            },
          ],
        },
      ],
    },
  },
];

export default a;
