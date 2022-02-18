module.exports = {
  docs: [
    {'Smart Data Lake' : ['intro', 'features']},
    {'Getting Started' : [
      'getting-started/setup',
      'getting-started/get-input-data',
      {
        'Part 1': [
          'getting-started/part-1/get-departures',
          'getting-started/part-1/get-airports',
          'getting-started/part-1/select-columns',
          'getting-started/part-1/joining-it-together',
          'getting-started/part-1/joining-departures-and-arrivals',
          'getting-started/part-1/compute-distances'
        ],
        'Part 2': [
            'getting-started/part-2/industrializing',
            'getting-started/part-2/delta-lake-format',
            'getting-started/part-2/historical-data'
        ],
        'Part 3': [
          'getting-started/part-3/custom-webservice',
          'getting-started/part-3/incremental-mode'
        ],
        'Troubleshooting': [
          'getting-started/troubleshooting/common-problems',
          'getting-started/troubleshooting/docker-on-windows'
        ]
      }

    ]},
    {'Reference' : [
        'reference/build',
        'reference/commandLine',
        //'reference/hoconOverview',
        //'reference/hoconElements',
        //'reference/dag',
        'reference/executionPhases',
        //'reference/executionModes',
        //'reference/transformations',
        //'reference/schemaEvolution',
        //'reference/housekeeping',
        //'reference/streaming',
        //'reference/metrics',
        //'reference/deploymentOptions',
        //'reference/testing',
        'reference/troubleshooting',
        //'reference/glossary'
    ]},
    {
      type: 'link',
      label: 'Configuration Schema Viewer', // The link label
      href: '/JsonSchemaViewer', // The internal path
    },
  ],
};