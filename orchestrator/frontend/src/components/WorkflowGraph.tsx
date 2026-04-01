import { useMemo, useState, useEffect } from 'react';
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  NodeTypes,
  EdgeTypes,
  Handle,
  Position,
  EdgeProps,
  getSmoothStepPath,
} from 'reactflow';
import 'reactflow/dist/style.css';
import dagre from '@dagrejs/dagre';
import { useWorkflowStore } from '../stores/workflowStore';
import { Task, Phase } from '../types/workflow';
import {
  CheckCircle,
  XCircle,
  Loader2,
  Clock,
  SkipForward,
  Shield,
  Activity,
  Database,
  Table2,
  Search,
  Brain,
  Zap,
  Cpu,
  Timer,
} from 'lucide-react';

const NODE_WIDTH = 240;
const NODE_HEIGHT = 100;

const statusConfig = {
  pending: { bg: 'bg-slate-800/80', border: 'border-slate-600/50', text: 'text-slate-400', ring: '' },
  running: { bg: 'bg-slate-900/90', border: 'border-cyan-400', text: 'text-cyan-300', ring: 'ring-2 ring-cyan-400/30 ring-offset-2 ring-offset-slate-950' },
  success: { bg: 'bg-slate-900/90', border: 'border-emerald-400', text: 'text-emerald-300', ring: '' },
  failed: { bg: 'bg-slate-900/90', border: 'border-red-400', text: 'text-red-300', ring: 'ring-2 ring-red-400/20' },
  skipped: { bg: 'bg-slate-900/80', border: 'border-amber-500/50', text: 'text-amber-300', ring: '' },
};

const phaseColors = [
  { accent: '#06b6d4', bg: 'rgba(6,182,212,0.06)', border: 'rgba(6,182,212,0.15)' },
  { accent: '#8b5cf6', bg: 'rgba(139,92,246,0.06)', border: 'rgba(139,92,246,0.15)' },
  { accent: '#f59e0b', bg: 'rgba(245,158,11,0.06)', border: 'rgba(245,158,11,0.15)' },
  { accent: '#10b981', bg: 'rgba(16,185,129,0.06)', border: 'rgba(16,185,129,0.15)' },
  { accent: '#ef4444', bg: 'rgba(239,68,68,0.06)', border: 'rgba(239,68,68,0.15)' },
  { accent: '#ec4899', bg: 'rgba(236,72,153,0.06)', border: 'rgba(236,72,153,0.15)' },
];

const skillTypeColors: Record<string, string> = {
  standalone: 'bg-purple-500/20 text-purple-300 border border-purple-500/30',
  platform: 'bg-blue-500/20 text-blue-300 border border-blue-500/30',
  infrastructure: 'bg-slate-500/20 text-slate-300 border border-slate-500/30',
  routing: 'bg-amber-500/20 text-amber-300 border border-amber-500/30',
};

const StatusIcon = ({ status }: { status: Task['status'] }) => {
  switch (status) {
    case 'success':
      return <CheckCircle className="w-4 h-4 text-emerald-400" />;
    case 'failed':
      return <XCircle className="w-4 h-4 text-red-400" />;
    case 'running':
      return <Loader2 className="w-4 h-4 text-cyan-400 animate-spin" />;
    case 'skipped':
      return <SkipForward className="w-4 h-4 text-amber-400" />;
    default:
      return <Clock className="w-4 h-4 text-slate-500" />;
  }
};

function getArtifactBadges(artifacts: Record<string, unknown>) {
  const badges: { icon: typeof Database; label: string; color: string }[] = [];
  if (!artifacts || Object.keys(artifacts).length === 0) return badges;

  if (artifacts.dynamic_tables && Array.isArray(artifacts.dynamic_tables)) {
    badges.push({ icon: Database, label: `${(artifacts.dynamic_tables as string[]).length} DTs`, color: 'bg-cyan-500/20 text-cyan-300' });
  }
  if (artifacts.tables_created && Array.isArray(artifacts.tables_created)) {
    badges.push({ icon: Table2, label: `${(artifacts.tables_created as string[]).length} tables`, color: 'bg-emerald-500/20 text-emerald-300' });
  }
  if (artifacts.search_service) {
    badges.push({ icon: Search, label: 'Search', color: 'bg-amber-500/20 text-amber-300' });
  }
  if (artifacts.model_name) {
    badges.push({ icon: Brain, label: 'ML Model', color: 'bg-purple-500/20 text-purple-300' });
  }
  if (artifacts.views && Array.isArray(artifacts.views)) {
    badges.push({ icon: Table2, label: `${(artifacts.views as string[]).length} views`, color: 'bg-blue-500/20 text-blue-300' });
  }
  if (artifacts.row_count && typeof artifacts.row_count === 'number') {
    badges.push({ icon: Table2, label: `${artifacts.row_count} rows`, color: 'bg-slate-500/20 text-slate-300' });
  }
  if (artifacts.signals_detected && typeof artifacts.signals_detected === 'number') {
    badges.push({ icon: Shield, label: `${artifacts.signals_detected} signals`, color: 'bg-red-500/20 text-red-300' });
  }
  if (artifacts.doc_count && typeof artifacts.doc_count === 'number') {
    badges.push({ icon: Table2, label: `${artifacts.doc_count} docs`, color: 'bg-violet-500/20 text-violet-300' });
  }
  return badges;
}

function AnimatedEdge({
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
}: EdgeProps) {
  const status = (data?.status as string) || 'pending';

  const [edgePath] = getSmoothStepPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    borderRadius: 16,
  });

  const isRunning = status === 'running';
  const isSuccess = status === 'success';
  const isFailed = status === 'failed';
  const isPending = status === 'pending';

  const strokeColor = isRunning ? '#06b6d4' : isSuccess ? '#10b981' : isFailed ? '#ef4444' : '#334155';
  const glowColor = isRunning ? '#06b6d4' : isSuccess ? '#10b981' : 'transparent';

  return (
    <g>
      {(isRunning || isSuccess) && (
        <path
          d={edgePath}
          fill="none"
          stroke={glowColor}
          strokeWidth={8}
          strokeOpacity={0.15}
          className={isRunning ? 'animate-edge-glow' : ''}
        />
      )}

      <path
        d={edgePath}
        fill="none"
        stroke={strokeColor}
        strokeWidth={isPending ? 1.5 : 2.5}
        strokeDasharray={isPending ? '6 4' : undefined}
        strokeOpacity={isPending ? 0.4 : 1}
        markerEnd={`url(#arrow-${status})`}
      />

      {isRunning && (
        <>
          <circle r="3.5" fill="#06b6d4">
            <animateMotion dur="1.5s" repeatCount="indefinite" path={edgePath} />
          </circle>
          <circle r="3.5" fill="#06b6d4" opacity="0.6">
            <animateMotion dur="1.5s" repeatCount="indefinite" path={edgePath} begin="0.5s" />
          </circle>
          <circle r="3.5" fill="#06b6d4" opacity="0.3">
            <animateMotion dur="1.5s" repeatCount="indefinite" path={edgePath} begin="1s" />
          </circle>
          <circle r="6" fill="#06b6d4" opacity="0.15">
            <animateMotion dur="1.5s" repeatCount="indefinite" path={edgePath} />
          </circle>
        </>
      )}

      {isSuccess && (
        <circle r="2.5" fill="#10b981" opacity="0.5">
          <animateMotion dur="3s" repeatCount="indefinite" path={edgePath} />
        </circle>
      )}
    </g>
  );
}

function TaskNode({ data }: { data: Task & { phaseIndex?: number } }) {
  const colors = statusConfig[data.status];
  const { setSelectedTask, setShowLogModal } = useWorkflowStore();

  const handleClick = () => {
    setSelectedTask(data.id);
    setShowLogModal(true);
  };

  const skillBadge = data.skill_type
    ? skillTypeColors[data.skill_type] || 'bg-slate-500/20 text-slate-300'
    : null;

  const artifactBadges = data.status === 'success' ? getArtifactBadges(data.artifacts) : [];
  const phaseColor = phaseColors[(data.phaseIndex ?? 0) % phaseColors.length];

  return (
    <div
      onClick={handleClick}
      className={`relative px-4 py-3 rounded-xl border-2 ${colors.bg} ${colors.border} ${colors.ring} cursor-pointer backdrop-blur-sm transition-all duration-300 hover:scale-[1.02] hover:brightness-110 min-w-[220px]`}
      style={{
        boxShadow: data.status === 'running'
          ? `0 0 20px ${phaseColor.accent}33, 0 0 40px ${phaseColor.accent}11`
          : data.status === 'success'
          ? '0 0 12px rgba(16,185,129,0.15)'
          : 'none',
      }}
    >
      <Handle type="target" position={Position.Left} className="!bg-slate-600 !w-2 !h-2 !border-slate-500" />

      <div className="flex items-center gap-2 mb-1.5">
        <StatusIcon status={data.status} />
        <span className={`font-semibold text-sm leading-tight ${colors.text}`}>{data.name}</span>
      </div>

      {data.skill_name && (
        <div className="flex items-center gap-1.5 mt-1">
          {data.skill_type === 'platform' ? (
            <Shield className="w-3 h-3 text-blue-400" />
          ) : (
            <Activity className="w-3 h-3 text-purple-400" />
          )}
          <span className="text-[10px] text-slate-500">{data.skill_name}</span>
          {skillBadge && (
            <span className={`text-[9px] px-1.5 py-0.5 rounded-full ${skillBadge}`}>
              {data.skill_type}
            </span>
          )}
        </div>
      )}

      {artifactBadges.length > 0 && (
        <div className="flex flex-wrap gap-1 mt-2">
          {artifactBadges.map((badge, i) => {
            const Icon = badge.icon;
            return (
              <span key={i} className={`inline-flex items-center gap-1 text-[9px] px-1.5 py-0.5 rounded-full ${badge.color}`}>
                <Icon className="w-2.5 h-2.5" />
                {badge.label}
              </span>
            );
          })}
        </div>
      )}

      {data.status === 'running' && (
        <div className="mt-2">
          <div className="h-1.5 bg-slate-700/50 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full bg-gradient-to-r from-cyan-500 to-cyan-300 transition-all duration-500"
              style={{ width: `${data.progress}%` }}
            />
          </div>
          <div className="flex items-center justify-between mt-1">
            <span className="text-[10px] text-cyan-400 font-mono">{data.progress}%</span>
            <span className="text-[10px] text-slate-500 flex items-center gap-0.5">
              <Zap className="w-2.5 h-2.5" />
              processing
            </span>
          </div>
        </div>
      )}

      {data.duration !== null && data.status === 'success' && (
        <div className="flex items-center gap-1 mt-1.5">
          <Timer className="w-3 h-3 text-emerald-500" />
          <span className="text-[10px] text-emerald-400 font-mono">{data.duration.toFixed(1)}s</span>
        </div>
      )}

      {data.error && (
        <p className="text-[10px] text-red-400 mt-1 truncate max-w-[200px]" title={data.error}>
          {data.error}
        </p>
      )}

      <Handle type="source" position={Position.Right} className="!bg-slate-600 !w-2 !h-2 !border-slate-500" />
    </div>
  );
}

const nodeTypes: NodeTypes = { task: TaskNode };
const edgeTypes: EdgeTypes = { animated: AnimatedEdge };

function getLayoutedElements(
  nodes: Node[],
  edges: Edge[],
  direction: 'TB' | 'LR' = 'LR'
): { nodes: Node[]; edges: Edge[] } {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: direction, nodesep: 50, ranksep: 120, marginx: 60, marginy: 40 });

  nodes.forEach((node) => {
    g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  });

  edges.forEach((edge) => {
    g.setEdge(edge.source, edge.target);
  });

  dagre.layout(g);

  const layoutedNodes = nodes.map((node) => {
    const pos = g.node(node.id);
    return {
      ...node,
      position: { x: pos.x - NODE_WIDTH / 2, y: pos.y - NODE_HEIGHT / 2 },
    };
  });

  return { nodes: layoutedNodes, edges };
}

function HUD({ phases }: { phases: Phase[] }) {
  const [elapsed, setElapsed] = useState(0);
  const allTasks = useMemo(() => phases.flatMap((p) => p.tasks), [phases]);
  const total = allTasks.length;
  const completed = allTasks.filter((t) => t.status === 'success').length;
  const running = allTasks.filter((t) => t.status === 'running').length;
  const failed = allTasks.filter((t) => t.status === 'failed').length;
  const isActive = running > 0 || allTasks.some((t) => t.status === 'running');

  useEffect(() => {
    if (!isActive && completed === 0) return;
    const iv = setInterval(() => setElapsed((e) => e + 1), 1000);
    return () => clearInterval(iv);
  }, [isActive, completed]);

  const pct = total > 0 ? Math.round((completed / total) * 100) : 0;

  const formatTime = (s: number) => {
    const m = Math.floor(s / 60);
    const sec = s % 60;
    return `${m.toString().padStart(2, '0')}:${sec.toString().padStart(2, '0')}`;
  };

  return (
    <div className="absolute top-3 left-3 z-10 flex items-center gap-3">
      <div className="flex items-center gap-2 bg-slate-900/90 backdrop-blur-md border border-slate-700/50 rounded-lg px-3 py-1.5">
        <Cpu className="w-3.5 h-3.5 text-cyan-400" />
        <span className="text-[11px] font-mono text-slate-300">{formatTime(elapsed)}</span>
      </div>

      <div className="flex items-center gap-2 bg-slate-900/90 backdrop-blur-md border border-slate-700/50 rounded-lg px-3 py-1.5">
        <div className="w-16 h-1.5 bg-slate-700 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full bg-gradient-to-r from-cyan-500 to-emerald-400 transition-all duration-700"
            style={{ width: `${pct}%` }}
          />
        </div>
        <span className="text-[11px] font-mono text-slate-300">{completed}/{total}</span>
      </div>

      {running > 0 && (
        <div className="flex items-center gap-1.5 bg-cyan-950/80 backdrop-blur-md border border-cyan-800/50 rounded-lg px-3 py-1.5">
          <div className="w-1.5 h-1.5 rounded-full bg-cyan-400 animate-pulse" />
          <span className="text-[11px] font-mono text-cyan-300">{running} active</span>
        </div>
      )}

      {failed > 0 && (
        <div className="flex items-center gap-1.5 bg-red-950/80 backdrop-blur-md border border-red-800/50 rounded-lg px-3 py-1.5">
          <XCircle className="w-3 h-3 text-red-400" />
          <span className="text-[11px] font-mono text-red-300">{failed} failed</span>
        </div>
      )}

      {completed === total && total > 0 && (
        <div className="flex items-center gap-1.5 bg-emerald-950/80 backdrop-blur-md border border-emerald-800/50 rounded-lg px-3 py-1.5">
          <CheckCircle className="w-3 h-3 text-emerald-400" />
          <span className="text-[11px] font-mono text-emerald-300">complete</span>
        </div>
      )}
    </div>
  );
}

function PhaseLegend({ phases }: { phases: Phase[] }) {
  return (
    <div className="absolute bottom-3 left-3 z-10 flex items-center gap-2">
      {phases.map((phase, i) => {
        const color = phaseColors[i % phaseColors.length];
        const taskCount = phase.tasks.length;
        const done = phase.tasks.filter((t) => t.status === 'success').length;
        return (
          <div
            key={phase.id}
            className="flex items-center gap-1.5 bg-slate-900/90 backdrop-blur-md border rounded-lg px-2.5 py-1.5"
            style={{ borderColor: color.border }}
          >
            <div className="w-2 h-2 rounded-full" style={{ backgroundColor: color.accent, opacity: done === taskCount && taskCount > 0 ? 1 : 0.5 }} />
            <span className="text-[10px] text-slate-400">{phase.name}</span>
            <span className="text-[9px] font-mono text-slate-500">{done}/{taskCount}</span>
          </div>
        );
      })}
    </div>
  );
}

export function WorkflowGraph() {
  const { phases } = useWorkflowStore();

  const { nodes, edges } = useMemo(() => {
    const rawNodes: Node[] = [];
    const rawEdges: Edge[] = [];

    phases.forEach((phase, phaseIndex) => {
      phase.tasks.forEach((task) => {
        rawNodes.push({
          id: task.id,
          type: 'task',
          position: { x: 0, y: 0 },
          data: { ...task, phaseIndex },
        });

        task.dependencies.forEach((depId) => {
          const depStatus = phases.flatMap((p) => p.tasks).find((t) => t.id === depId)?.status || 'pending';
          const targetStatus = task.status;

          let edgeStatus = 'pending';
          if (targetStatus === 'running') edgeStatus = 'running';
          else if (targetStatus === 'success' && depStatus === 'success') edgeStatus = 'success';
          else if (targetStatus === 'failed') edgeStatus = 'failed';

          rawEdges.push({
            id: `${depId}-${task.id}`,
            source: depId,
            target: task.id,
            type: 'animated',
            data: { status: edgeStatus },
          });
        });
      });
    });

    if (rawNodes.length === 0) return { nodes: [], edges: [] };

    return getLayoutedElements(rawNodes, rawEdges, 'LR');
  }, [phases]);

  return (
    <div className="h-full w-full relative">
      <style>{`
        @keyframes pulse-glow {
          0%, 100% { box-shadow: 0 0 8px rgba(6,182,212,0.3); }
          50% { box-shadow: 0 0 25px rgba(6,182,212,0.6); }
        }
        @keyframes success-pop {
          0% { transform: scale(1); }
          50% { transform: scale(1.04); }
          100% { transform: scale(1); }
        }
        @keyframes shake {
          0%, 100% { transform: translateX(0); }
          10%, 30%, 50%, 70%, 90% { transform: translateX(-2px); }
          20%, 40%, 60%, 80% { transform: translateX(2px); }
        }
        @keyframes edge-glow {
          0%, 100% { stroke-opacity: 0.1; }
          50% { stroke-opacity: 0.25; }
        }
        .animate-pulse-glow { animation: pulse-glow 2s ease-in-out infinite; }
        .animate-success-pop { animation: success-pop 0.4s ease-out; }
        .animate-shake { animation: shake 0.5s ease-in-out; }
        .animate-edge-glow { animation: edge-glow 2s ease-in-out infinite; }
      `}</style>

      <svg style={{ position: 'absolute', width: 0, height: 0 }}>
        <defs>
          <marker id="arrow-running" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#06b6d4" />
          </marker>
          <marker id="arrow-success" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#10b981" />
          </marker>
          <marker id="arrow-failed" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#ef4444" />
          </marker>
          <marker id="arrow-pending" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#334155" />
          </marker>
        </defs>
      </svg>

      <HUD phases={phases} />
      <PhaseLegend phases={phases} />

      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{ padding: 0.25 }}
        minZoom={0.2}
        maxZoom={1.5}
        defaultEdgeOptions={{ type: 'animated' }}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#1e293b" gap={24} size={1} />
        <Controls className="!bg-slate-900/90 !backdrop-blur-md !border-slate-700/50 !rounded-lg !shadow-xl" />
      </ReactFlow>
    </div>
  );
}
