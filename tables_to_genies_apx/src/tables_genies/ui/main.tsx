import React from 'react';
import ReactDOM from 'react-dom/client';
import { RouterProvider, createRouter } from '@tanstack/react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { routeTree } from './routeTree.gen';
import './index.css';

// Create router
const router = createRouter({ routeTree });

// Create query client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Suspense disabled - causes issues with TanStack Router in dev
      suspense: false,
      refetchOnWindowFocus: false,
      refetchOnMount: true,
      staleTime: 0, // Always consider data stale, refetch on mount
      retry: 1,
    },
  },
});

// Register router type
declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </React.StrictMode>,
);
