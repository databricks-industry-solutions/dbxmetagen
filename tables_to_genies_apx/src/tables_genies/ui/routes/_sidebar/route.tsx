import { createFileRoute, useNavigate, Link, Outlet } from '@tanstack/react-router';
import { Database, Sparkles, Share2, Boxes, Rocket, ChevronLeft, ChevronRight, AlertTriangle, X } from 'lucide-react';
import { useState, useCallback } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { clearAllWorkflowState } from '@/lib/workflow-state';
import customInstance from '@/lib/axios-instance';

export const Route = createFileRoute('/_sidebar')({
  component: SidebarLayout,
});

const navItems = [
  { to: '/catalog-browser', label: '1. Browse Catalogs', icon: <Database size={16} /> },
  { to: '/enrichment', label: '2. Enrich Tables', icon: <Sparkles size={16} /> },
  { to: '/graph-explorer', label: '3. Explore Graph', icon: <Share2 size={16} /> },
  { to: '/genie-builder', label: '4. Build Rooms', icon: <Boxes size={16} /> },
  { to: '/genie-create', label: '5. Create Rooms', icon: <Rocket size={16} /> },
];

function SidebarLayout() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  const [resetting, setResetting] = useState(false);
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const handleReset = useCallback(async () => {
    setResetting(true);
    try {
      await customInstance({ url: '/api/reset', method: 'POST' });
    } catch (e) {
      console.error('Failed to reset server state:', e);
    }
    clearAllWorkflowState();
    queryClient.clear();
    setShowResetConfirm(false);
    navigate({ to: '/catalog-browser' });
    window.location.reload();
  }, [queryClient, navigate]);

  return (
    <div className="flex h-screen bg-white dark:bg-slate-950 text-slate-900 dark:text-slate-50">
      {/* Sidebar */}
      <aside className={`${
        sidebarCollapsed ? 'w-16' : 'w-64'
      } bg-slate-50 dark:bg-slate-900 border-r border-slate-200 dark:border-slate-700 transition-all duration-300 relative flex flex-col`}>
        {/* Toggle Button */}
        <button
          onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
          className="absolute -right-3 top-6 bg-blue-600 text-white rounded-full p-1 shadow-lg hover:bg-blue-700 transition-colors z-20"
          title={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          {sidebarCollapsed ? <ChevronRight size={16} /> : <ChevronLeft size={16} />}
        </button>

        <div className="p-6 flex-1 overflow-y-auto">
          {!sidebarCollapsed && (
            <>
              <h1 className="text-2xl font-bold mb-2">Tables to Genies</h1>
              <p className="text-sm text-slate-600 dark:text-slate-400 mb-6">Create Genie rooms from UC tables</p>
            </>
          )}
          
          <nav className="space-y-2">
            {navItems.map((item) => (
              <Link
                key={item.to}
                to={item.to}
                className={`flex items-center gap-3 px-4 py-2 rounded-md hover:bg-slate-200 dark:hover:bg-slate-800 transition-colors text-slate-700 dark:text-slate-300 ${
                  sidebarCollapsed ? 'justify-center' : ''
                }`}
                activeProps={{
                  className: 'bg-blue-600 text-white hover:bg-blue-700 dark:hover:bg-blue-700',
                }}
                title={sidebarCollapsed ? item.label : undefined}
              >
                {item.icon}
                {!sidebarCollapsed && <span className="text-sm font-medium">{item.label}</span>}
              </Link>
            ))}
          </nav>
        </div>

        {/* Sidebar Footer */}
        <div className="p-4 border-t border-slate-200 dark:border-slate-700">
          <button
            onClick={() => setShowResetConfirm(true)}
            className={`flex items-center gap-3 w-full px-4 py-2 rounded-md text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors ${
              sidebarCollapsed ? 'justify-center' : ''
            }`}
            title={sidebarCollapsed ? 'Reset Workflow' : undefined}
          >
            <Rocket size={16} className="rotate-180" />
            {!sidebarCollapsed && <span className="text-sm font-medium">Reset Workflow</span>}
          </button>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-auto bg-white dark:bg-slate-950">
        <div className="container mx-auto p-8">
          <Outlet />
        </div>
      </main>

      {/* Reset Confirmation Modal */}
      {showResetConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center">
          {/* Backdrop */}
          <div
            className="absolute inset-0 bg-black/50 backdrop-blur-sm"
            onClick={() => !resetting && setShowResetConfirm(false)}
          />
          {/* Dialog */}
          <div className="relative bg-white dark:bg-slate-900 rounded-xl shadow-2xl border border-slate-200 dark:border-slate-700 w-full max-w-md mx-4 overflow-hidden">
            {/* Header */}
            <div className="flex items-start gap-4 p-6 pb-2">
              <div className="flex-shrink-0 w-11 h-11 rounded-full bg-red-100 dark:bg-red-900/30 flex items-center justify-center">
                <AlertTriangle className="w-6 h-6 text-red-600 dark:text-red-400" />
              </div>
              <div className="flex-1 min-w-0">
                <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
                  Reset Entire Workflow?
                </h3>
              </div>
              <button
                onClick={() => setShowResetConfirm(false)}
                disabled={resetting}
                className="flex-shrink-0 p-1 rounded-md text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors disabled:opacity-50"
              >
                <X size={18} />
              </button>
            </div>

            {/* Body */}
            <div className="px-6 pb-4">
              <p className="text-sm text-slate-600 dark:text-slate-400 mb-4">
                This will <span className="font-semibold text-red-600 dark:text-red-400">permanently clear all progress</span> and reset every page back to scratch:
              </p>
              <ul className="space-y-2 text-sm">
                <li className="flex items-center gap-2 text-slate-700 dark:text-slate-300">
                  <Database size={14} className="text-slate-400 flex-shrink-0" />
                  <span><strong>Browse Catalogs</strong> — all table selections cleared</span>
                </li>
                <li className="flex items-center gap-2 text-slate-700 dark:text-slate-300">
                  <Sparkles size={14} className="text-slate-400 flex-shrink-0" />
                  <span><strong>Enrich Tables</strong> — enrichment job state cleared</span>
                </li>
                <li className="flex items-center gap-2 text-slate-700 dark:text-slate-300">
                  <Share2 size={14} className="text-slate-400 flex-shrink-0" />
                  <span><strong>Explore Graph</strong> — graph data removed</span>
                </li>
                <li className="flex items-center gap-2 text-slate-700 dark:text-slate-300">
                  <Boxes size={14} className="text-slate-400 flex-shrink-0" />
                  <span><strong>Build Rooms</strong> — all room definitions deleted</span>
                </li>
                <li className="flex items-center gap-2 text-slate-700 dark:text-slate-300">
                  <Rocket size={14} className="text-slate-400 flex-shrink-0" />
                  <span><strong>Create Rooms</strong> — creation progress cleared</span>
                </li>
              </ul>
            </div>

            {/* Footer */}
            <div className="flex items-center justify-end gap-3 px-6 py-4 bg-slate-50 dark:bg-slate-800/50 border-t border-slate-200 dark:border-slate-700">
              <button
                onClick={() => setShowResetConfirm(false)}
                disabled={resetting}
                className="px-4 py-2 text-sm font-medium text-slate-700 dark:text-slate-300 bg-white dark:bg-slate-800 border border-slate-300 dark:border-slate-600 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                onClick={handleReset}
                disabled={resetting}
                className="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-lg hover:bg-red-700 transition-colors disabled:opacity-70 flex items-center gap-2"
              >
                {resetting ? (
                  <>
                    <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    Resetting…
                  </>
                ) : (
                  'Yes, Reset Everything'
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
