import { createRootRoute, Outlet } from '@tanstack/react-router';
import { useEffect } from 'react';

export const Route = createRootRoute({
  component: () => {
    useEffect(() => {
      console.log('Root route mounted');
    }, []);
    
    return (
      <div className="min-h-screen bg-white dark:bg-slate-950 text-slate-900 dark:text-slate-50">
        <Outlet />
      </div>
    );
  },
});
