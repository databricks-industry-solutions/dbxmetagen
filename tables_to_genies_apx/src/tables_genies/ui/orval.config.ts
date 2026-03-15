import { defineConfig } from 'orval';

export default defineConfig({
  api: {
    input: 'http://localhost:8000/openapi.json',
    output: {
      mode: 'single',
      target: './lib/api.ts',
      client: 'react-query',
      override: {
        mutator: {
          path: './lib/axios-instance.ts',
          name: 'customInstance',
        },
        query: {
          useQuery: true,
          useSuspenseQuery: true,
        },
      },
    },
  },
});
