import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { Suspense, useState, useEffect, useRef } from 'react';
import { useGetSelectionSuspense, useRunEnrichment, useGetEnrichmentStatusSuspense } from '@/lib/api';
import { selector } from '@/lib/selector';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Skeleton } from '@/components/ui/skeleton';
import { ArrowLeft, ExternalLink } from 'lucide-react';
import { loadState, saveState, isStepCompleted, markStepCompleted } from '@/lib/workflow-state';

export const Route = createFileRoute('/_sidebar/enrichment')({
  validateSearch: (search: Record<string, unknown>) => {
    return {
      jobId: search.jobId ? Number(search.jobId) : undefined,
    };
  },
  component: () => {
    const { jobId } = Route.useSearch();
    return (
      <div>
        <h1 className="text-3xl font-bold mb-6">Enrich Tables</h1>
        <Suspense fallback={<EnrichmentSkeleton />}>
          <EnrichmentView initialJobId={jobId} />
        </Suspense>
      </div>
    );
  },
});

function EnrichmentView({ initialJobId }: { initialJobId?: number }) {
  const { data: selection } = useGetSelectionSuspense(selector());
  const [jobId, setJobId] = useState<number | null>(initialJobId || null);
  const [jobUrl, setJobUrl] = useState<string | null>(null);
  const [metadataTable, setMetadataTable] = useState('serverless_dbx_unifiedchat_catalog.gold.enriched_table_metadata');
  const [chunksTable, setChunksTable] = useState('serverless_dbx_unifiedchat_catalog.gold.enriched_table_chunks');
  const [writeMode, setWriteMode] = useState<'overwrite' | 'append' | 'error'>('overwrite');
  const isLoadedRef = useRef(false);
  const runEnrichmentMutation = useRunEnrichment();
  const navigate = useNavigate();
  const tablesSelected = isStepCompleted('tables-selected');

  // Load state on mount
  useEffect(() => {
    const savedState = loadState('enrichment');
    if (savedState) {
      if (!initialJobId && savedState.jobId) {
        setJobId(savedState.jobId);
        setJobUrl(savedState.jobUrl);
      }
      setMetadataTable(savedState.metadataTable);
      setChunksTable(savedState.chunksTable);
      setWriteMode(savedState.writeMode);
    }
    // Set isLoadedRef after a short delay to ensure state updates have processed
    const timer = setTimeout(() => {
      isLoadedRef.current = true;
    }, 100);
    return () => clearTimeout(timer);
  }, [initialJobId]);

  // Save state on changes
  useEffect(() => {
    if (!isLoadedRef.current) return;

    saveState('enrichment', {
      jobId,
      jobUrl,
      metadataTable,
      chunksTable,
      writeMode,
    });

    // Update URL if jobId changes
    if (jobId) {
      navigate({
        search: (prev) => ({ ...prev, jobId }),
        replace: true,
      });
    }
  }, [jobId, jobUrl, metadataTable, chunksTable, writeMode, navigate]);

  const handleRunEnrichment = async () => {
    const result = await runEnrichmentMutation.mutateAsync({
      data: { 
        table_fqns: selection.table_fqns,
        metadata_table: metadataTable,
        chunks_table: chunksTable,
        write_mode: writeMode
      }
    });
    // result now contains: {run_id, job_url, status}
    setJobId(result.run_id);
    setJobUrl(result.job_url);
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Selected Tables ({selection.count})</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2 mb-4">
            {selection.table_fqns.slice(0, 10).map((fqn) => (
              <div key={fqn} className="text-sm p-2 bg-accent rounded">{fqn}</div>
            ))}
            {selection.count > 10 && (
              <p className="text-sm text-muted-foreground">...and {selection.count - 10} more</p>
            )}
          </div>

          {!jobId && (
            <div className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="metadata-table">Metadata Table (Destination)</Label>
                  <Input
                    id="metadata-table"
                    value={metadataTable}
                    onChange={(e) => setMetadataTable(e.target.value)}
                    placeholder="catalog.schema.table"
                    className="font-mono text-sm"
                  />
                  <p className="text-xs text-muted-foreground">
                    Enriched table metadata will be written here
                  </p>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="chunks-table">Chunks Table (Destination)</Label>
                  <Input
                    id="chunks-table"
                    value={chunksTable}
                    onChange={(e) => setChunksTable(e.target.value)}
                    placeholder="catalog.schema.table"
                    className="font-mono text-sm"
                  />
                  <p className="text-xs text-muted-foreground">
                    Table/column chunks for vector search will be written here
                  </p>
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="write-mode">Write Mode</Label>
                <Select value={writeMode} onValueChange={(value: any) => setWriteMode(value)}>
                  <SelectTrigger id="write-mode" className="w-full md:w-64">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="overwrite">Overwrite (replace existing data)</SelectItem>
                    <SelectItem value="append">Append (add to existing data)</SelectItem>
                    <SelectItem value="error">Error (fail if table exists)</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <Button onClick={handleRunEnrichment} disabled={runEnrichmentMutation.isPending || !tablesSelected}>
                {runEnrichmentMutation.isPending ? 'Starting...' : 'Run Enrichment'}
              </Button>
              {!tablesSelected && (
                <p className="text-xs text-amber-600 dark:text-amber-400">
                  Complete step 1 (Browse Catalogs) first to enable enrichment.
                </p>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {jobId && jobUrl && (
        <Suspense fallback={<Skeleton className="h-32 w-full" />}>
          <EnrichmentProgress 
            jobId={jobId} 
            jobUrl={jobUrl}
            metadataTable={metadataTable}
            chunksTable={chunksTable}
          />
        </Suspense>
      )}

      <div className="flex gap-4">
        <Button variant="outline" onClick={() => navigate({ to: '/catalog-browser' })}>
          <ArrowLeft size={16} /> Back
        </Button>
        {jobId && isStepCompleted('enrichment-done') && (
          <Button onClick={() => navigate({ to: '/graph-explorer' })}>
            Next: Explore Graph →
          </Button>
        )}
      </div>
    </div>
  );
}

function EnrichmentProgress({ 
  jobId, 
  jobUrl, 
  metadataTable, 
  chunksTable 
}: { 
  jobId: number; 
  jobUrl: string;
  metadataTable: string;
  chunksTable: string;
}) {
  const { data: status } = useGetEnrichmentStatusSuspense(jobId, {
    query: {
      refetchInterval: (query) => {
        const data = query.state.data;
        // Poll every 5 seconds if job is still running
        if (data && (data.status === 'pending' || data.status === 'running')) {
          return 5000;
        }
        return false;
      }
    }
  });

  useEffect(() => {
    if (status.status === 'completed') {
      markStepCompleted('enrichment-done');
    }
  }, [status.status]);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between">
          <span>Enrichment Progress</span>
          <a 
            href={jobUrl} 
            target="_blank" 
            rel="noopener noreferrer"
            className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 flex items-center gap-1 transition-colors"
          >
            View Job in Databricks <ExternalLink size={14} />
          </a>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          <div className="flex justify-between text-sm">
            <span className="font-medium">Status: <span className={
              status.status === 'completed' ? 'text-green-600 dark:text-green-400' :
              status.status === 'failed' ? 'text-red-600 dark:text-red-400' :
              status.status === 'running' ? 'text-blue-600 dark:text-blue-400' :
              'text-slate-600 dark:text-slate-400'
            }>{status.status.toUpperCase()}</span></span>
            <span className="text-slate-600 dark:text-slate-400">Run ID: {status.run_id}</span>
          </div>
          
          {status.life_cycle_state && (
            <div className="text-xs text-slate-600 dark:text-slate-400 bg-slate-50 dark:bg-slate-800 rounded p-2">
              <div>Lifecycle: <span className="font-mono">{status.life_cycle_state}</span></div>
              {status.result_state && (
                <div>Result: <span className="font-mono">{status.result_state}</span></div>
              )}
            </div>
          )}
          
          {status.duration_ms && (
            <div className="text-sm text-slate-600 dark:text-slate-400">
              Duration: <span className="font-semibold">{(status.duration_ms / 1000).toFixed(1)}s</span>
            </div>
          )}
          
          {status.state_message && (
            <p className="text-sm text-slate-600 dark:text-slate-400 italic bg-slate-50 dark:bg-slate-800 rounded p-2">
              {status.state_message}
            </p>
          )}
          
          {status.status === 'failed' && (
            <div className="text-sm text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded p-3 border border-red-200 dark:border-red-800">
              <p className="font-semibold">Enrichment job failed</p>
              <p className="text-xs mt-1">Check the job logs in Databricks for details.</p>
            </div>
          )}
          
          {status.status === 'running' && (
            <div className="flex items-center gap-2 text-sm text-blue-600 dark:text-blue-400">
              <div className="animate-spin h-4 w-4 border-2 border-blue-600 dark:border-blue-400 border-t-transparent rounded-full"></div>
              <span>Job is running...</span>
            </div>
          )}
          
          {status.status === 'completed' && (
            <>
              <div className="text-sm text-green-600 dark:text-green-400 bg-green-50 dark:bg-green-900/20 rounded p-3 border border-green-200 dark:border-green-800">
                ✓ Enrichment completed successfully!
              </div>
              
              <div className="mt-4 space-y-3">
                <div className="text-sm font-semibold text-slate-700 dark:text-slate-300">Output Tables:</div>
                
                <div className="flex flex-col gap-2">
                  <a
                    href={`https://fevm-serverless-dbx-unifiedchat.cloud.databricks.com/explore/data/${metadataTable.split('.')[0]}/${metadataTable.split('.')[1]}/${metadataTable.split('.')[2]}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-2 px-4 py-3 bg-slate-50 dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg border border-slate-200 dark:border-slate-700 transition-colors"
                  >
                    <ExternalLink size={16} className="text-blue-600 dark:text-blue-400" />
                    <div className="flex-1">
                      <div className="text-sm font-medium text-slate-900 dark:text-slate-100">Metadata Table</div>
                      <div className="text-xs font-mono text-slate-600 dark:text-slate-400">{metadataTable}</div>
                    </div>
                  </a>
                  
                  <a
                    href={`https://fevm-serverless-dbx-unifiedchat.cloud.databricks.com/explore/data/${chunksTable.split('.')[0]}/${chunksTable.split('.')[1]}/${chunksTable.split('.')[2]}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-2 px-4 py-3 bg-slate-50 dark:bg-slate-800 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg border border-slate-200 dark:border-slate-700 transition-colors"
                  >
                    <ExternalLink size={16} className="text-blue-600 dark:text-blue-400" />
                    <div className="flex-1">
                      <div className="text-sm font-medium text-slate-900 dark:text-slate-100">Chunks Table</div>
                      <div className="text-xs font-mono text-slate-600 dark:text-slate-400">{chunksTable}</div>
                    </div>
                  </a>
                </div>
              </div>
            </>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function EnrichmentSkeleton() {
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <Skeleton className="h-8 w-48" />
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {[...Array(5)].map((_, i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
