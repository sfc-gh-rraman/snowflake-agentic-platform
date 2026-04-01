import { useEffect, useState } from 'react';
import { useWorkflowStore } from '../stores/workflowStore';
import {
  ChevronDown,
  ChevronRight,
  Database,
  Table2,
  Search,
  BarChart3,
  ExternalLink,
  Package,
  Eye,
  Clock,
  X,
  Loader2,
} from 'lucide-react';

const typeIcons: Record<string, typeof Database> = {
  'DYNAMIC TABLE': Database,
  TABLE: Table2,
  VIEW: BarChart3,
  'SEARCH SERVICE': Search,
};

const schemaColors: Record<string, string> = {
  ANALYTICS: 'text-cyan-400',
  ML: 'text-purple-400',
  CORTEX: 'text-amber-400',
  DRUG_SAFETY: 'text-red-400',
  CLINICAL_DOCS: 'text-violet-400',
  APPS: 'text-green-400',
};

interface FreshnessInfo {
  NAME: string;
  SCHEMA_NAME: string;
  MINUTES_SINCE_REFRESH: number | null;
  REFRESH_STATUS: string | null;
}

interface PreviewData {
  columns: string[];
  rows: Record<string, string | null>[];
}

export function ArtifactExplorer() {
  const { artifacts, setArtifacts, artifactsPanelOpen, toggleArtifactsPanel, phases } =
    useWorkflowStore();
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [freshness, setFreshness] = useState<Record<string, FreshnessInfo>>({});
  const [preview, setPreview] = useState<{ schema: string; table: string; data: PreviewData | null; loading: boolean } | null>(null);

  const completedTasks = phases.flatMap((p) => p.tasks).filter((t) => t.status === 'success');

  useEffect(() => {
    if (completedTasks.length === 0) return;
    const collected: typeof artifacts = [];
    for (const task of completedTasks) {
      if (task.artifacts) {
        const a = task.artifacts as Record<string, unknown>;
        if (a.tables_created && Array.isArray(a.tables_created)) {
          for (const t of a.tables_created as string[]) {
            collected.push({
              schema: (a.schema as string) || 'UNKNOWN',
              name: t,
              type: (a.type as string) || 'TABLE',
              row_count: (a.row_counts as Record<string, number>)?.[t] ?? null,
              bytes: null,
              created_on: null,
              task_id: task.id,
            });
          }
        }
        if (a.dynamic_tables && Array.isArray(a.dynamic_tables)) {
          for (const dt of a.dynamic_tables as string[]) {
            collected.push({
              schema: 'ANALYTICS',
              name: dt,
              type: 'DYNAMIC TABLE',
              row_count: (a.row_counts as Record<string, number>)?.[dt] ?? null,
              bytes: null,
              created_on: null,
              task_id: task.id,
            });
          }
        }
        if (a.search_service) {
          collected.push({
            schema: (a.schema as string) || 'CORTEX',
            name: a.search_service as string,
            type: 'SEARCH SERVICE',
            row_count: (a.corpus_size as number) ?? null,
            bytes: null,
            created_on: null,
            task_id: task.id,
          });
        }
        if (a.model_name) {
          collected.push({
            schema: (a.schema as string) || 'ML',
            name: a.model_name as string,
            type: 'ML MODEL',
            row_count: (a.predictions_count as number) ?? null,
            bytes: null,
            created_on: null,
            task_id: task.id,
          });
        }
        if (a.table_name && !a.tables_created) {
          collected.push({
            schema: (a.schema as string) || 'UNKNOWN',
            name: a.table_name as string,
            type: (a.type as string) || 'TABLE',
            row_count: (a.row_count as number) ?? null,
            bytes: null,
            created_on: null,
            task_id: task.id,
          });
        }
        if (a.views && Array.isArray(a.views)) {
          for (const v of a.views as string[]) {
            collected.push({
              schema: (a.schema as string) || 'UNKNOWN',
              name: v,
              type: 'VIEW',
              row_count: null,
              bytes: null,
              created_on: null,
              task_id: task.id,
            });
          }
        }
      }
    }
    const unique = collected.filter(
      (item, idx, arr) =>
        arr.findIndex((a) => a.schema === item.schema && a.name === item.name) === idx
    );
    setArtifacts(unique);
  }, [completedTasks.length, setArtifacts]);

  useEffect(() => {
    if (artifacts.length === 0) return;
    const fetchFreshness = async () => {
      try {
        const res = await fetch('/api/workflow/data-freshness');
        if (res.ok) {
          const data = await res.json();
          const map: Record<string, FreshnessInfo> = {};
          for (const dt of data.dynamic_tables || []) {
            map[dt.NAME] = dt;
          }
          setFreshness(map);
        }
      } catch {}
    };
    fetchFreshness();
    const interval = setInterval(fetchFreshness, 30000);
    return () => clearInterval(interval);
  }, [artifacts.length]);

  const fetchPreview = async (schema: string, name: string) => {
    const parts = name.split('.');
    const tableName = parts[parts.length - 1];
    setPreview({ schema, table: tableName, data: null, loading: true });
    try {
      const res = await fetch(`/api/workflow/data-preview/${schema}/${tableName}?limit=5`);
      if (res.ok) {
        const data = await res.json();
        setPreview({ schema, table: tableName, data: { columns: data.columns, rows: data.rows }, loading: false });
      } else {
        setPreview(null);
      }
    } catch {
      setPreview(null);
    }
  };

  const grouped = artifacts.reduce<Record<string, typeof artifacts>>((acc, a) => {
    if (!acc[a.schema]) acc[a.schema] = [];
    acc[a.schema].push(a);
    return acc;
  }, {});

  const toggleSchema = (s: string) => {
    setExpandedSchemas((prev) => {
      const next = new Set(prev);
      if (next.has(s)) next.delete(s);
      else next.add(s);
      return next;
    });
  };

  const getFreshnessBadge = (name: string) => {
    const shortName = name.split('.').pop() || name;
    const info = freshness[shortName];
    if (!info || info.MINUTES_SINCE_REFRESH === null) return null;
    const mins = info.MINUTES_SINCE_REFRESH;
    const color = mins <= 60 ? 'text-green-400' : mins <= 180 ? 'text-amber-400' : 'text-red-400';
    return (
      <span className={`text-[9px] ${color} flex items-center gap-0.5`} title={`Last refresh: ${mins}m ago`}>
        <Clock className="w-2.5 h-2.5" />
        {mins < 60 ? `${mins}m` : `${Math.round(mins / 60)}h`}
      </span>
    );
  };

  if (!artifactsPanelOpen) {
    return (
      <button
        onClick={toggleArtifactsPanel}
        className="fixed bottom-4 left-4 z-40 flex items-center gap-2 px-3 py-2 bg-slate-800 border border-slate-700 rounded-lg text-xs text-slate-300 hover:bg-slate-700 transition-colors"
      >
        <Package className="w-4 h-4 text-cyan-400" />
        Artifacts ({artifacts.length})
      </button>
    );
  }

  return (
    <div className="fixed bottom-0 left-0 z-40 w-80 max-h-[50vh] bg-slate-900 border-t border-r border-slate-700 rounded-tr-xl flex flex-col shadow-2xl">
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-800">
        <div className="flex items-center gap-2">
          <Package className="w-4 h-4 text-cyan-400" />
          <span className="text-sm font-semibold text-white">Snowflake Artifacts</span>
          <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-cyan-900 text-cyan-300">
            {artifacts.length}
          </span>
        </div>
        <button onClick={toggleArtifactsPanel} className="p-1 hover:bg-slate-800 rounded">
          <ChevronDown className="w-4 h-4 text-slate-400" />
        </button>
      </div>

      {preview && (
        <div className="border-b border-slate-800 p-2 bg-slate-950">
          <div className="flex items-center justify-between mb-1">
            <span className="text-[10px] text-cyan-400 font-medium">{preview.schema}.{preview.table}</span>
            <button onClick={() => setPreview(null)} className="p-0.5 hover:bg-slate-800 rounded">
              <X className="w-3 h-3 text-slate-500" />
            </button>
          </div>
          {preview.loading ? (
            <div className="flex items-center justify-center py-4">
              <Loader2 className="w-4 h-4 text-cyan-400 animate-spin" />
            </div>
          ) : preview.data ? (
            <div className="overflow-x-auto max-h-32">
              <table className="text-[9px] w-full">
                <thead>
                  <tr>
                    {preview.data.columns.map((c) => (
                      <th key={c} className="text-left px-1 py-0.5 text-slate-400 border-b border-slate-800 whitespace-nowrap">{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.data.rows.map((row, i) => (
                    <tr key={i} className="hover:bg-slate-800/50">
                      {preview.data!.columns.map((c) => (
                        <td key={c} className="px-1 py-0.5 text-slate-300 whitespace-nowrap max-w-[100px] truncate">{row[c] ?? '-'}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
      )}

      <div className="flex-1 overflow-y-auto p-2 space-y-1">
        {Object.keys(grouped).length === 0 ? (
          <p className="text-xs text-slate-500 text-center py-4">
            Run a scenario to see created objects
          </p>
        ) : (
          Object.entries(grouped).map(([schema, items]) => (
            <div key={schema}>
              <button
                onClick={() => toggleSchema(schema)}
                className="flex items-center gap-2 w-full px-2 py-1.5 rounded hover:bg-slate-800 text-left"
              >
                {expandedSchemas.has(schema) ? (
                  <ChevronDown className="w-3 h-3 text-slate-500" />
                ) : (
                  <ChevronRight className="w-3 h-3 text-slate-500" />
                )}
                <span className={`text-xs font-medium ${schemaColors[schema] || 'text-slate-300'}`}>
                  {schema}
                </span>
                <span className="text-[10px] text-slate-500">{items.length}</span>
              </button>
              {expandedSchemas.has(schema) && (
                <div className="ml-5 space-y-0.5">
                  {items.map((item) => {
                    const Icon = typeIcons[item.type] || Table2;
                    return (
                      <div
                        key={`${item.schema}.${item.name}`}
                        className="flex items-center justify-between px-2 py-1 rounded hover:bg-slate-800/50 group"
                      >
                        <div className="flex items-center gap-1.5 min-w-0">
                          <Icon className="w-3 h-3 text-slate-500 shrink-0" />
                          <span className="text-xs text-slate-300 truncate">{item.name}</span>
                          <span className="text-[9px] px-1 py-0 rounded bg-slate-800 text-slate-500">
                            {item.type}
                          </span>
                        </div>
                        <div className="flex items-center gap-2 shrink-0">
                          {getFreshnessBadge(item.name)}
                          {item.row_count !== null && (
                            <span className="text-[10px] text-slate-500">{item.row_count} rows</span>
                          )}
                          <button
                            onClick={() => fetchPreview(item.schema, item.name)}
                            className="opacity-0 group-hover:opacity-100 p-0.5 hover:bg-slate-700 rounded"
                            title="Preview data"
                          >
                            <Eye className="w-3 h-3 text-cyan-400" />
                          </button>
                          <ExternalLink className="w-3 h-3 text-slate-600 opacity-0 group-hover:opacity-100 cursor-pointer" />
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
