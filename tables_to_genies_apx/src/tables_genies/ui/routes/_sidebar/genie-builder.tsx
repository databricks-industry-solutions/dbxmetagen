import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { Suspense, useState, useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useGetGraphDataSuspense, useCreateGenieRoom, useListGenieRoomsSuspense, useDeleteGenieRoom, useUpdateGenieRoom } from '@/lib/api';
import { selector } from '@/lib/selector';
import { loadState, saveState, isStepCompleted, markStepCompleted } from '@/lib/workflow-state';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { ArrowLeft, X, Database, Sparkles, Plus, Trash2, CheckCircle2, Edit2, Check } from 'lucide-react';

export const Route = createFileRoute('/_sidebar/genie-builder')({
  component: () => {
    const graphReady = isStepCompleted('graph-built');

    return (
      <div className="relative">
        {/* Modern Header with Gradient */}
        <div className="mb-8">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 text-white shadow-lg">
              <Sparkles className="w-6 h-6" />
            </div>
            <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
              Build Genie Rooms
            </h1>
          </div>
          <p className="text-slate-600 dark:text-slate-400 ml-14">
            Group related tables together to create focused data exploration spaces
          </p>
        </div>

        {graphReady ? (
          <Suspense fallback={<BuilderSkeleton />}>
            <GenieBuilderView />
          </Suspense>
        ) : (
          <StepNotReady />
        )}
      </div>
    );
  },
});

function GenieBuilderView() {
  const queryClient = useQueryClient();
  const { data: graphData } = useGetGraphDataSuspense(selector());
  const { data: rooms } = useListGenieRoomsSuspense(selector());
  const [selectedTableFqns, setSelectedTableFqns] = useState<string[]>([]);
  const [roomName, setRoomName] = useState('');
  const [editingRoomId, setEditingRoomId] = useState<string | null>(null);
  const [editingRoomName, setEditingRoomName] = useState('');
  const isLoadedRef = useRef(false);
  const graphBuilt = isStepCompleted('graph-built');

  // Load state on mount
  useEffect(() => {
    const savedState = loadState('genie-builder');
    if (savedState) {
      setRoomName(savedState.roomName);
      setSelectedTableFqns(savedState.selectedTableFqns);
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

    saveState('genie-builder', {
      roomName,
      selectedTableFqns,
    });
  }, [roomName, selectedTableFqns]);
  
  const createRoomMutation = useCreateGenieRoom({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ['/api/genie/rooms'] });
      }
    }
  });

  const updateRoomMutation = useUpdateGenieRoom({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ['/api/genie/rooms'] });
        setEditingRoomId(null);
      }
    }
  });
  
  const deleteRoomMutation = useDeleteGenieRoom({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ['/api/genie/rooms'] });
      }
    }
  });
  
  const navigate = useNavigate();

  const tableNodes = graphData.elements.filter((elem: any) => !elem.data.source);

  const handleAddRoom = async () => {
    if (!roomName || selectedTableFqns.length === 0) return;

    await createRoomMutation.mutateAsync({
      data: {
        name: roomName,
        table_fqns: selectedTableFqns,
      }
    });

    markStepCompleted('rooms-defined');
    setRoomName('');
    setSelectedTableFqns([]);
  };

  const handleDeleteRoom = async (roomId: string) => {
    await deleteRoomMutation.mutateAsync({
      roomId: roomId
    });
  };

  const handleUpdateRoomName = async (roomId: string) => {
    if (!editingRoomName.trim()) return;
    await updateRoomMutation.mutateAsync({
      roomId,
      data: { name: editingRoomName.trim() }
    });
  };

  const hasUnsavedChanges = roomName.trim() !== '' || selectedTableFqns.length > 0;

  useEffect(() => {
    const handleBeforeUnload = (e: BeforeUnloadEvent) => {
      if (hasUnsavedChanges) {
        e.preventDefault();
        e.returnValue = '';
      }
    };
    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => window.removeEventListener('beforeunload', handleBeforeUnload);
  }, [hasUnsavedChanges]);

  const startEditing = (room: any) => {
    setEditingRoomId(room.id);
    setEditingRoomName(room.name);
  };

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        {/* Table Selection - Takes 3 columns */}
        <Card className="lg:col-span-3 shadow-lg hover:shadow-xl transition-shadow duration-300 border-slate-200 dark:border-slate-700">
          <CardHeader className="bg-gradient-to-r from-blue-50 to-purple-50 dark:from-slate-800 dark:to-slate-800 border-b">
            <div className="flex items-center gap-2">
              <Database className="w-5 h-5 text-blue-600 dark:text-blue-400" />
              <CardTitle className="text-lg">Select Tables for Room</CardTitle>
            </div>
          </CardHeader>
          <CardContent className="space-y-5 pt-6">
            {/* Room Name Input */}
            <div className="space-y-2">
              <label className="text-sm font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-2">
                <Sparkles className="w-4 h-4 text-purple-500" />
                Room Name
              </label>
              <input
                type="text"
                value={roomName}
                onChange={(e) => setRoomName(e.target.value)}
                placeholder="e.g., Patient Records, Clinical Data..."
                className="w-full px-4 py-3 border-2 border-slate-200 dark:border-slate-700 rounded-lg 
                         focus:border-blue-500 focus:ring-2 focus:ring-blue-200 dark:focus:ring-blue-900
                         transition-all duration-200 bg-white dark:bg-slate-900
                         placeholder:text-slate-400 text-sm font-medium"
              />
            </div>

            {/* Table Selection */}
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <label className="text-sm font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-2">
                  <Database className="w-4 h-4 text-blue-500" />
                  Select Tables
                </label>
                {selectedTableFqns.length > 0 && (
                  <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 text-xs font-semibold">
                    <CheckCircle2 className="w-3 h-3" />
                    {selectedTableFqns.length} selected
                  </span>
                )}
              </div>
              
              <div className="relative overflow-hidden rounded-lg border-2 border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/50">
                <div className="overflow-x-auto">
                  <select
                    multiple
                    value={selectedTableFqns}
                    onChange={(e) => setSelectedTableFqns(Array.from(e.target.selectedOptions, opt => opt.value))}
                    className="w-full h-56 px-4 py-3 border-0 text-xs font-mono whitespace-nowrap
                             bg-transparent focus:outline-none focus:ring-0
                             [&>option]:py-2 [&>option]:px-2 [&>option]:rounded-md [&>option]:my-0.5
                             [&>option:checked]:bg-blue-500 [&>option:checked]:text-white
                             [&>option:hover]:bg-slate-200 dark:[&>option:hover]:bg-slate-700"
                    style={{ minWidth: '600px' }}
                  >
                    {tableNodes.map((node: any) => (
                      <option key={node.data.id} value={node.data.id} title={node.data.id}>
                        {node.data.id}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
              
              <p className="text-xs text-slate-500 dark:text-slate-400 flex items-center gap-1.5">
                <span className="inline-block w-1.5 h-1.5 rounded-full bg-blue-500"></span>
                Hold Cmd/Ctrl to select multiple tables
              </p>
            </div>

            {/* Add Room Button */}
            <Button
              onClick={handleAddRoom}
              disabled={!roomName || selectedTableFqns.length === 0 || createRoomMutation.isPending || !graphBuilt}
              className="w-full bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700
                       text-white font-semibold py-6 rounded-lg shadow-md hover:shadow-lg
                       transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed
                       disabled:hover:shadow-md"
            >
              <Plus className="w-5 h-5 mr-2" />
              {createRoomMutation.isPending ? 'Adding Room...' : 'Add Room'}
            </Button>
            {!graphBuilt && (
              <p className="text-xs text-amber-600 dark:text-amber-400 text-center">
                Complete step 3 (Explore Graph) first to enable room building.
              </p>
            )}
          </CardContent>
        </Card>

        {/* Planned Rooms - Takes 2 columns */}
        <Card className="lg:col-span-2 shadow-lg hover:shadow-xl transition-shadow duration-300 border-slate-200 dark:border-slate-700">
          <CardHeader className="bg-gradient-to-r from-purple-50 to-pink-50 dark:from-slate-800 dark:to-slate-800 border-b">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div className="p-1.5 rounded-md bg-purple-100 dark:bg-purple-900/30">
                  <Sparkles className="w-4 h-4 text-purple-600 dark:text-purple-400" />
                </div>
                <CardTitle className="text-lg">Planned Rooms</CardTitle>
              </div>
              <span className="px-2.5 py-1 rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-400 text-xs font-bold">
                {rooms.length}
              </span>
            </div>
          </CardHeader>
          <CardContent className="pt-4">
            {rooms.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12 text-center">
                <div className="w-16 h-16 rounded-full bg-slate-100 dark:bg-slate-800 flex items-center justify-center mb-4">
                  <Database className="w-8 h-8 text-slate-400" />
                </div>
                <p className="text-sm font-medium text-slate-600 dark:text-slate-400 mb-1">No rooms yet</p>
                <p className="text-xs text-slate-500 dark:text-slate-500">Create your first room to get started</p>
              </div>
            ) : (
              <div className="space-y-3 max-h-[calc(100vh-400px)] overflow-y-auto pr-2">
                {rooms.map((room, index) => (
                  <div 
                    key={room.id} 
                    className="group relative border-2 border-slate-200 dark:border-slate-700 rounded-lg p-4
                             hover:border-purple-300 dark:hover:border-purple-700 
                             hover:shadow-md transition-all duration-200
                             bg-white dark:bg-slate-900"
                  >
                    <div className="flex justify-between items-start gap-3">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-2">
                          <span className="flex-shrink-0 w-6 h-6 rounded-full bg-gradient-to-br from-purple-500 to-pink-500 
                                         text-white text-xs font-bold flex items-center justify-center">
                            {index + 1}
                          </span>
                          {editingRoomId === room.id ? (
                            <div className="flex items-center gap-1 flex-1 min-w-0">
                              <input
                                type="text"
                                value={editingRoomName}
                                onChange={(e) => setEditingRoomName(e.target.value)}
                                onKeyDown={(e) => {
                                  if (e.key === 'Enter') handleUpdateRoomName(room.id);
                                  if (e.key === 'Escape') setEditingRoomId(null);
                                }}
                                autoFocus
                                className="flex-1 px-2 py-1 text-sm border rounded dark:bg-slate-800 dark:border-slate-600 focus:outline-none focus:ring-1 focus:ring-purple-500"
                              />
                              <Button
                                size="icon"
                                variant="ghost"
                                onClick={() => handleUpdateRoomName(room.id)}
                                className="h-7 w-7 text-green-600 hover:text-green-700 hover:bg-green-50"
                              >
                                <Check size={14} />
                              </Button>
                              <Button
                                size="icon"
                                variant="ghost"
                                onClick={() => setEditingRoomId(null)}
                                className="h-7 w-7 text-slate-400 hover:text-slate-600"
                              >
                                <X size={14} />
                              </Button>
                            </div>
                          ) : (
                            <h4 className="font-semibold text-slate-900 dark:text-slate-100 truncate flex-1">
                              {room.name}
                            </h4>
                          )}
                        </div>
                        <div className="flex items-center gap-1.5 text-sm text-slate-600 dark:text-slate-400">
                          <Database className="w-3.5 h-3.5" />
                          <span className="font-medium">{room.table_count || 0}</span>
                          <span>tables</span>
                        </div>
                      </div>
                      
                      <div className="flex items-center gap-1">
                        {editingRoomId !== room.id && (
                          <Button
                            variant="ghost"
                            onClick={() => startEditing(room)}
                            className="h-8 w-8 p-0 rounded-lg opacity-0 group-hover:opacity-100 transition-opacity
                                     hover:bg-blue-100 dark:hover:bg-blue-900/30 hover:text-blue-600 dark:hover:text-blue-400"
                            title="Rename room"
                          >
                            <Edit2 size={14} />
                          </Button>
                        )}
                        <Button
                          variant="ghost"
                          onClick={() => handleDeleteRoom(room.id)}
                          className="h-8 w-8 p-0 rounded-lg opacity-0 group-hover:opacity-100 transition-opacity
                                   hover:bg-red-100 dark:hover:bg-red-900/30 hover:text-red-600 dark:hover:text-red-400"
                          title="Delete room"
                        >
                          <Trash2 size={14} />
                        </Button>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Navigation Buttons */}
      <div className="flex gap-4 pt-4 border-t border-slate-200 dark:border-slate-700">
        <Button 
          variant="outline" 
          onClick={() => navigate({ to: '/graph-explorer' })}
          className="px-6 py-5 font-semibold hover:bg-slate-100 dark:hover:bg-slate-800"
        >
          <ArrowLeft size={18} className="mr-2" />
          Back to Graph
        </Button>
        <Button 
          onClick={() => navigate({ to: '/genie-create' })} 
          disabled={rooms.length === 0 || !isStepCompleted('rooms-defined')}
          className="px-6 py-5 font-semibold bg-gradient-to-r from-green-600 to-emerald-600 
                   hover:from-green-700 hover:to-emerald-700 text-white
                   disabled:opacity-50 disabled:cursor-not-allowed shadow-md hover:shadow-lg
                   transition-all duration-200"
        >
          Next: Create Rooms
          <ArrowLeft size={18} className="ml-2 rotate-180" />
        </Button>
      </div>
    </div>
  );
}

function StepNotReady() {
  const navigate = useNavigate();
  return (
    <div className="space-y-6">
      <Card className="border-amber-200 dark:border-amber-800">
        <CardContent className="flex flex-col items-center justify-center py-16 text-center">
          <div className="w-16 h-16 rounded-full bg-amber-100 dark:bg-amber-900/30 flex items-center justify-center mb-4">
            <Database className="w-8 h-8 text-amber-500" />
          </div>
          <p className="text-lg font-semibold text-slate-700 dark:text-slate-300 mb-2">Graph Not Built Yet</p>
          <p className="text-sm text-slate-500 dark:text-slate-400 max-w-md">
            Complete the previous steps first: browse catalogs, enrich tables, and build the graph before creating Genie rooms.
          </p>
          <Button
            variant="outline"
            className="mt-6"
            onClick={() => navigate({ to: '/graph-explorer' })}
          >
            <ArrowLeft size={16} className="mr-2" />
            Go to Explore Graph
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}

function BuilderSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        <div className="lg:col-span-3 space-y-4">
          <Skeleton className="h-[500px] w-full rounded-lg" />
        </div>
        <div className="lg:col-span-2 space-y-4">
          <Skeleton className="h-[500px] w-full rounded-lg" />
        </div>
      </div>
      <div className="flex gap-4">
        <Skeleton className="h-12 w-40" />
        <Skeleton className="h-12 w-48" />
      </div>
    </div>
  );
}
