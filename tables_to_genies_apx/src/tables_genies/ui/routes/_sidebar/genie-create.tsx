import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { Suspense, useState, useEffect, useRef } from 'react';
import { useListGenieRoomsSuspense, useCreateAllGenieRooms, useGetGenieCreationStatus, useListCreatedGenieRooms } from '@/lib/api';
import { selector } from '@/lib/selector';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { ArrowLeft, ExternalLink, Boxes } from 'lucide-react';
import { loadState, saveState, isStepCompleted } from '@/lib/workflow-state';

export const Route = createFileRoute('/_sidebar/genie-create')({
  component: () => {
    const roomsReady = isStepCompleted('rooms-defined');

    return (
      <div>
        <h1 className="text-3xl font-bold mb-6">Create Genie Rooms</h1>
        {roomsReady ? (
          <Suspense fallback={<CreateSkeleton />}>
            <GenieCreateView />
          </Suspense>
        ) : (
          <CreateStepNotReady />
        )}
      </div>
    );
  },
});

function GenieCreateView() {
  const { data: rooms } = useListGenieRoomsSuspense(selector());
  const [creationStarted, setCreationStarted] = useState(false);
  const isLoadedRef = useRef(false);
  const createAllMutation = useCreateAllGenieRooms();
  const navigate = useNavigate();
  const roomsDefined = isStepCompleted('rooms-defined');

  // Load state on mount
  useEffect(() => {
    const savedState = loadState('genie-create');
    if (savedState) {
      setCreationStarted(savedState.creationStarted);
    }
    // Set isLoadedRef after a short delay to ensure state updates have processed
    const timer = setTimeout(() => {
      isLoadedRef.current = true;
    }, 100);
    return () => clearTimeout(timer);
  }, []);

  // Save state on changes
  useEffect(() => {
    if (!isLoadedRef.current) return;

    saveState('genie-create', {
      creationStarted,
    });
  }, [creationStarted]);

  const handleCreateAll = async () => {
    await createAllMutation.mutateAsync();
    setCreationStarted(true);
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Ready to Create {rooms.length} Genie Rooms</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="space-y-2">
              {rooms.map((room) => (
                <div key={room.id} className="flex justify-between items-center p-3 bg-accent rounded-lg">
                  <div>
                    <p className="font-medium">{room.name}</p>
                    <p className="text-sm text-muted-foreground">{room.table_count} tables</p>
                  </div>
                </div>
              ))}
            </div>

            {!creationStarted && (
              <div className="space-y-2">
                <Button onClick={handleCreateAll} disabled={createAllMutation.isPending || !roomsDefined}>
                  {createAllMutation.isPending ? 'Creating...' : 'Create All Rooms'}
                </Button>
                {!roomsDefined && (
                  <p className="text-xs text-amber-600 dark:text-amber-400">
                    Complete step 4 (Build Rooms) first to enable room creation.
                  </p>
                )}
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {creationStarted && (
        <Suspense fallback={<Skeleton className="h-64 w-full" />}>
          <CreationProgress />
        </Suspense>
      )}

      {creationStarted && (
        <Suspense fallback={<Skeleton className="h-64 w-full" />}>
          <CreatedRooms />
        </Suspense>
      )}

      <div className="flex gap-4">
        <Button variant="outline" onClick={() => navigate({ to: '/genie-builder' })}>
          <ArrowLeft size={16} /> Back
        </Button>
      </div>
    </div>
  );
}

function CreationProgress() {
  const { data: status } = useGetGenieCreationStatus({
    query: {
      select: selector(),
      refetchInterval: 2000, // Poll every 2 seconds
    }
  });

  if (!status) return <Skeleton className="h-64 w-full" />;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between">
          <span>Creation Progress</span>
          {status.status === 'creating' && (
            <span className="flex h-2 w-2 relative">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500"></span>
            </span>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-2">
          {status.rooms.map((room: any) => (
            <div key={room.id} className="p-3 border rounded-lg transition-all hover:bg-accent/50">
              <div className="flex items-center justify-between mb-2">
                <span className="font-medium">{room.name}</span>
                <span className={`text-xs px-2 py-1 rounded-full font-semibold uppercase tracking-wider ${
                  room.status === 'created' ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400' :
                  room.status === 'creating' ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400' :
                  room.status === 'failed' ? 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400' :
                  'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400'
                }`}>
                  {room.status}
                </span>
              </div>
              {room.status === 'failed' && room.error && (
                <div className="mt-2 p-2 bg-red-50 dark:bg-red-950/40 rounded text-sm text-red-800 dark:text-red-300 border border-red-200/50 dark:border-red-800/50">
                  <div className="flex items-start gap-2">
                    <span className="mt-1 text-red-500">
                      <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
                    </span>
                    <div className="flex-1">
                      <p className="font-semibold text-xs mb-1">Error Details:</p>
                      <p className="text-[10px] font-mono break-all leading-relaxed opacity-90">{room.error}</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function CreatedRooms() {
  const { data: createdRooms } = useListCreatedGenieRooms({
    query: {
      select: selector(),
      refetchInterval: 2000,
    }
  });

  if (!createdRooms || createdRooms.length === 0) {
    return null;
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>✓ Created Genie Rooms</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {createdRooms.map((room) => (
            <div key={room.id} className="border rounded-lg p-4">
              <div className="flex justify-between items-start">
                <div>
                  <h4 className="font-medium mb-1">{room.name}</h4>
                  <p className="text-sm text-muted-foreground">{room.table_count} tables</p>
                </div>
                <a
                  href={room.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-2 text-sm text-primary hover:underline"
                >
                  Open Genie Space <ExternalLink size={14} />
                </a>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function CreateStepNotReady() {
  const navigate = useNavigate();
  return (
    <div className="space-y-6">
      <Card className="border-amber-200 dark:border-amber-800">
        <CardContent className="flex flex-col items-center justify-center py-16 text-center">
          <div className="w-16 h-16 rounded-full bg-amber-100 dark:bg-amber-900/30 flex items-center justify-center mb-4">
            <Boxes className="w-8 h-8 text-amber-500" />
          </div>
          <p className="text-lg font-semibold text-slate-700 dark:text-slate-300 mb-2">No Rooms Defined Yet</p>
          <p className="text-sm text-slate-500 dark:text-slate-400 max-w-md">
            Complete the previous steps first: browse catalogs, enrich tables, build the graph, and define rooms before creating Genie spaces.
          </p>
          <Button
            variant="outline"
            className="mt-6"
            onClick={() => navigate({ to: '/genie-builder' })}
          >
            <ArrowLeft size={16} className="mr-2" />
            Go to Build Rooms
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}

function CreateSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-64 w-full" />
    </div>
  );
}
