import { useMemo } from 'react';
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  MarkerType,
  NodeTypes,
  Handle,
  Position,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { useWorkflowStore } from '../stores/workflowStore';
import { Task } from '../types/workflow';
import { CheckCircle, XCircle, Loader2, Clock, SkipForward, Shield, Activity } from 'lucide-react';

const statusColors = {
  pending: { bg: 'bg-slate-700', border: 'border-slate-600', text: 'text-slate-300' },
  running: { bg: 'bg-cyan-900', border: 'border-cyan-500', text: 'text-cyan-300' },
  success: { bg: 'bg-green-900', border: 'border-green-500', text: 'text-green-300' },
  failed: { bg: 'bg-red-900', border: 'border-red-500', text: 'text-red-300' },
  skipped: { bg: 'bg-yellow-900', border: 'border-yellow-500', text: 'text-yellow-300' },
};

const skillTypeColors: Record<string, string> = {
  standalone: 'bg-purple-800 text-purple-200',
  platform: 'bg-blue-800 text-blue-200',
  infrastructure: 'bg-slate-600 text-slate-200',
  routing: 'bg-amber-800 text-amber-200',
};

const StatusIcon = ({ status }: { status: Task['status'] }) => {
  switch (status) {
    case 'success':
      return <CheckCircle className="w-4 h-4 text-green-400" />;
    case 'failed':
      return <XCircle className="w-4 h-4 text-red-400" />;
    case 'running':
      return <Loader2 className="w-4 h-4 text-cyan-400 animate-spin" />;
    case 'skipped':
      return <SkipForward className="w-4 h-4 text-yellow-400" />;
    default:
      return <Clock className="w-4 h-4 text-slate-400" />;
  }
};

function TaskNode({ data }: { data: Task }) {
  const colors = statusColors[data.status];
  const { setSelectedTask, setShowLogModal } = useWorkflowStore();

  const handleClick = () => {
    setSelectedTask(data.id);
    setShowLogModal(true);
  };

  const skillBadge = data.skill_type
    ? skillTypeColors[data.skill_type] || 'bg-slate-600 text-slate-200'
    : null;

  return (
    <div
      onClick={handleClick}
      className={`px-4 py-3 rounded-lg border-2 ${colors.bg} ${colors.border} cursor-pointer hover:brightness-110 transition-all min-w-[200px]`}
    >
      <Handle type="target" position={Position.Top} className="!bg-slate-500" />

      <div className="flex items-center gap-2 mb-1">
        <StatusIcon status={data.status} />
        <span className={`font-medium text-sm ${colors.text}`}>{data.name}</span>
      </div>

      {data.skill_name && (
        <div className="flex items-center gap-1 mt-1">
          {data.skill_type === 'platform' ? (
            <Shield className="w-3 h-3 text-blue-400" />
          ) : (
            <Activity className="w-3 h-3 text-purple-400" />
          )}
          <span className="text-xs text-slate-400">{data.skill_name}</span>
          {skillBadge && (
            <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${skillBadge}`}>
              {data.skill_type}
            </span>
          )}
        </div>
      )}

      {data.status === 'running' && (
        <div className="mt-2">
          <div className="h-1 bg-slate-600 rounded-full overflow-hidden">
            <div
              className="h-full bg-cyan-400 transition-all duration-300"
              style={{ width: `${data.progress}%` }}
            />
          </div>
          <span className="text-xs text-slate-400 mt-1">{data.progress}%</span>
        </div>
      )}

      {data.duration !== null && data.status === 'success' && (
        <span className="text-xs text-slate-400">{data.duration.toFixed(1)}s</span>
      )}

      {data.error && (
        <p className="text-xs text-red-400 mt-1 truncate max-w-[180px]" title={data.error}>
          {data.error}
        </p>
      )}

      <Handle type="source" position={Position.Bottom} className="!bg-slate-500" />
    </div>
  );
}

const nodeTypes: NodeTypes = {
  task: TaskNode,
};

export function WorkflowGraph() {
  const { phases } = useWorkflowStore();

  const { nodes, edges } = useMemo(() => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];

    let yOffset = 50;

    phases.forEach((phase) => {
      nodes.push({
        id: `phase-${phase.id}`,
        type: 'default',
        position: { x: 30, y: yOffset },
        data: { label: phase.name },
        style: {
          background: 'transparent',
          border: 'none',
          color: '#94a3b8',
          fontSize: '14px',
          fontWeight: 'bold',
          width: 180,
        },
        draggable: false,
      });

      phase.tasks.forEach((task, taskIndex) => {
        const x = 230 + taskIndex * 240;
        const y = yOffset;

        nodes.push({
          id: task.id,
          type: 'task',
          position: { x, y },
          data: task,
        });

        task.dependencies.forEach((depId) => {
          edges.push({
            id: `${depId}-${task.id}`,
            source: depId,
            target: task.id,
            type: 'smoothstep',
            animated: task.status === 'running',
            style: {
              stroke: task.status === 'running' ? '#06b6d4' : '#475569',
              strokeWidth: 2,
            },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: task.status === 'running' ? '#06b6d4' : '#475569',
            },
          });
        });
      });

      yOffset += 130;
    });

    return { nodes, edges };
  }, [phases]);

  return (
    <div className="h-full w-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.4}
        maxZoom={1.5}
        defaultEdgeOptions={{
          type: 'smoothstep',
        }}
      >
        <Background color="#334155" gap={20} />
        <Controls className="!bg-slate-800 !border-slate-700 !rounded-lg" />
      </ReactFlow>
    </div>
  );
}
