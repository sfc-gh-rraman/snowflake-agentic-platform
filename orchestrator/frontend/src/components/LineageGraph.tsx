import { useMemo, useEffect } from 'react';
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  MarkerType,
  Handle,
  Position,
  NodeTypes,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { useWorkflowStore } from '../stores/workflowStore';
import { LineageNode } from '../types/workflow';
import { Database, Brain, Search, BarChart3, Layout, FileText } from 'lucide-react';

const typeConfig: Record<
  string,
  { color: string; bg: string; border: string; icon: typeof Database }
> = {
  raw: { color: 'text-slate-300', bg: 'bg-slate-800', border: 'border-slate-600', icon: Database },
  enriched: { color: 'text-cyan-300', bg: 'bg-cyan-900/50', border: 'border-cyan-600', icon: Database },
  ml: { color: 'text-purple-300', bg: 'bg-purple-900/50', border: 'border-purple-600', icon: Brain },
  search: { color: 'text-amber-300', bg: 'bg-amber-900/50', border: 'border-amber-600', icon: Search },
  analyst: { color: 'text-blue-300', bg: 'bg-blue-900/50', border: 'border-blue-600', icon: BarChart3 },
  app: { color: 'text-green-300', bg: 'bg-green-900/50', border: 'border-green-600', icon: Layout },
};

function LineageNodeComponent({ data }: { data: LineageNode & { label: string } }) {
  const cfg = typeConfig[data.type] || typeConfig.raw;
  const Icon = cfg.icon;

  return (
    <div className={`px-4 py-3 rounded-lg border-2 ${cfg.bg} ${cfg.border} min-w-[160px]`}>
      <Handle type="target" position={Position.Left} className="!bg-slate-500" />
      <div className="flex items-center gap-2">
        <Icon className={`w-4 h-4 ${cfg.color}`} />
        <span className={`text-sm font-medium ${cfg.color}`}>{data.label}</span>
      </div>
      <span className="text-[10px] text-slate-500">{data.schema}</span>
      <Handle type="source" position={Position.Right} className="!bg-slate-500" />
    </div>
  );
}

const nodeTypes: NodeTypes = { lineage: LineageNodeComponent };

const LINEAGE_CONFIGS: Record<string, { nodes: Omit<LineageNode, 'id'>[]; edges: [number, number][] }> = {
  clinical_data_warehouse: {
    nodes: [
      { label: 'PATIENT', type: 'raw', schema: 'FHIR_DEMO' },
      { label: 'OBSERVATION', type: 'raw', schema: 'FHIR_DEMO' },
      { label: 'CONDITION', type: 'raw', schema: 'FHIR_DEMO' },
      { label: 'PATIENT_ENRICHED', type: 'enriched', schema: 'ANALYTICS' },
      { label: 'OBSERVATION_ENRICHED', type: 'enriched', schema: 'ANALYTICS' },
      { label: 'CONDITION_ENRICHED', type: 'enriched', schema: 'ANALYTICS' },
      { label: 'PATIENT_360', type: 'enriched', schema: 'ANALYTICS' },
      { label: 'RISK_TRAINING_DATA', type: 'ml', schema: 'ML' },
      { label: 'PATIENT_RISK_CLASSIFIER', type: 'ml', schema: 'ML' },
      { label: 'RISK_PREDICTIONS', type: 'ml', schema: 'ML' },
      { label: 'CLINICAL_SEARCH_CORPUS', type: 'search', schema: 'CORTEX' },
      { label: 'CLINICAL_PATIENT_SEARCH', type: 'search', schema: 'CORTEX' },
      { label: 'Semantic Model', type: 'analyst', schema: 'CORTEX' },
      { label: 'Dashboard', type: 'app', schema: 'APPS' },
    ],
    edges: [
      [0, 3], [1, 4], [2, 5], [3, 6], [4, 6], [5, 6],
      [6, 7], [7, 8], [8, 9], [6, 10], [10, 11],
      [6, 12], [12, 13], [9, 13], [11, 13],
    ],
  },
  drug_safety: {
    nodes: [
      { label: 'FAERS_DEMO', type: 'raw', schema: 'DRUG_SAFETY' },
      { label: 'SIGNAL_DETECTION', type: 'enriched', schema: 'DRUG_SAFETY' },
      { label: 'SAFETY_DASHBOARD_SUMMARY', type: 'enriched', schema: 'DRUG_SAFETY' },
      { label: 'SAFETY_TEMPORAL_TRENDS', type: 'enriched', schema: 'DRUG_SAFETY' },
      { label: 'SIGNAL_SUMMARY', type: 'enriched', schema: 'DRUG_SAFETY' },
      { label: 'AI Assessment', type: 'analyst', schema: 'DRUG_SAFETY' },
    ],
    edges: [
      [0, 1], [1, 2], [1, 3], [1, 4], [4, 5],
    ],
  },
  clinical_docs: {
    nodes: [
      { label: 'DOCUMENT_REGISTRY', type: 'raw', schema: 'CLINICAL_DOCS' },
      { label: 'DOCUMENT_CONTENT', type: 'raw', schema: 'CLINICAL_DOCS' },
      { label: 'EXTRACTED_FIELDS', type: 'enriched', schema: 'CLINICAL_DOCS' },
      { label: 'DOC_SEARCH_CORPUS', type: 'search', schema: 'CLINICAL_DOCS' },
      { label: 'DOC_ANALYTICS', type: 'analyst', schema: 'CLINICAL_DOCS' },
      { label: 'Doc Search Service', type: 'search', schema: 'CLINICAL_DOCS' },
      { label: 'Doc Intelligence Agent', type: 'app', schema: 'CLINICAL_DOCS' },
    ],
    edges: [
      [0, 1], [1, 2], [2, 3], [3, 5], [2, 4], [5, 6], [4, 6],
    ],
  },
};

export function LineageGraph() {
  const { activeScenario, lineageNodes, lineageEdges, setLineage, phases } = useWorkflowStore();

  const hasCompleted = phases.some((p) => p.tasks.some((t) => t.status === 'success'));

  useEffect(() => {
    if (!activeScenario || !hasCompleted) return;
    const config = LINEAGE_CONFIGS[activeScenario];
    if (!config) return;

    const nodes = config.nodes.map((n, i) => ({
      id: `ln-${i}`,
      label: n.label,
      type: n.type,
      schema: n.schema,
    }));
    const edges = config.edges.map(([s, t]) => ({
      source: `ln-${s}`,
      target: `ln-${t}`,
    }));
    setLineage(nodes, edges);
  }, [activeScenario, hasCompleted, setLineage]);

  const { flowNodes, flowEdges } = useMemo(() => {
    const typeOrder = ['raw', 'enriched', 'ml', 'search', 'analyst', 'app'];
    const columns: Record<string, LineageNode[]> = {};
    lineageNodes.forEach((n) => {
      if (!columns[n.type]) columns[n.type] = [];
      columns[n.type].push(n);
    });

    const flowNodes: Node[] = [];
    typeOrder.forEach((type, colIdx) => {
      const col = columns[type] || [];
      col.forEach((n, rowIdx) => {
        flowNodes.push({
          id: n.id,
          type: 'lineage',
          position: { x: colIdx * 220 + 30, y: rowIdx * 90 + 60 },
          data: { ...n, label: n.label },
        });
      });

      if (col.length > 0) {
        flowNodes.push({
          id: `type-label-${type}`,
          type: 'default',
          position: { x: colIdx * 220 + 30, y: 10 },
          data: { label: type.toUpperCase() },
          style: {
            background: 'transparent',
            border: 'none',
            color: '#64748b',
            fontSize: '10px',
            fontWeight: 'bold',
            textTransform: 'uppercase' as const,
          },
          draggable: false,
        });
      }
    });

    const flowEdges: Edge[] = lineageEdges.map((e, i) => ({
      id: `le-${i}`,
      source: e.source,
      target: e.target,
      type: 'smoothstep',
      style: { stroke: '#475569', strokeWidth: 1.5 },
      markerEnd: { type: MarkerType.ArrowClosed, color: '#475569' },
    }));

    return { flowNodes, flowEdges };
  }, [lineageNodes, lineageEdges]);

  if (lineageNodes.length === 0) {
    return (
      <div className="h-full flex items-center justify-center bg-slate-950">
        <div className="text-center">
          <FileText className="w-8 h-8 text-slate-600 mx-auto mb-2" />
          <p className="text-sm text-slate-500">Run a scenario to see data lineage</p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full w-full">
      <ReactFlow
        nodes={flowNodes}
        edges={flowEdges}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.3 }}
        minZoom={0.3}
        maxZoom={1.5}
      >
        <Background color="#1e293b" gap={20} />
        <Controls className="!bg-slate-800 !border-slate-700 !rounded-lg" />
      </ReactFlow>
    </div>
  );
}
