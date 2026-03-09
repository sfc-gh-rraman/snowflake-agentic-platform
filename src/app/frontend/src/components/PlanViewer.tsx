import React, { useMemo } from 'react';
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  Position,
} from 'reactflow';
import 'reactflow/dist/style.css';
import type { Phase } from '../types';

interface PlanViewerProps {
  phases: Phase[];
}

const nodeColors: Record<string, string> = {
  running: '#29B5E8',
  completed: '#4CAF50',
  failed: '#f44336',
  pending: '#9e9e9e',
  retrying: '#FFC107',
};

const PlanViewer: React.FC<PlanViewerProps> = ({ phases }) => {
  const { nodes, edges } = useMemo(() => {
    const nodeList: Node[] = [];
    const edgeList: Edge[] = [];

    const nodeWidth = 200;
    const nodeHeight = 60;
    const horizontalGap = 80;
    const verticalGap = 100;

    const levelMap = new Map<string, number>();
    const assignLevels = (phaseId: string, level: number) => {
      const current = levelMap.get(phaseId) ?? -1;
      if (level > current) {
        levelMap.set(phaseId, level);
      }
    };

    const phaseMap = new Map(phases.map((p) => [p.phase_id, p]));

    phases.forEach((phase) => {
      if (!phase.depends_on || phase.depends_on.length === 0) {
        assignLevels(phase.phase_id, 0);
      }
    });

    let changed = true;
    while (changed) {
      changed = false;
      phases.forEach((phase) => {
        if (phase.depends_on && phase.depends_on.length > 0) {
          const maxDepLevel = Math.max(
            ...phase.depends_on.map((depId) => levelMap.get(depId) ?? -1)
          );
          if (maxDepLevel >= 0) {
            const newLevel = maxDepLevel + 1;
            if (newLevel > (levelMap.get(phase.phase_id) ?? -1)) {
              assignLevels(phase.phase_id, newLevel);
              changed = true;
            }
          }
        }
      });
    }

    phases.forEach((phase) => {
      if (!levelMap.has(phase.phase_id)) {
        levelMap.set(phase.phase_id, 0);
      }
    });

    const levelCounts = new Map<number, number>();
    phases.forEach((phase) => {
      const level = levelMap.get(phase.phase_id) ?? 0;
      levelCounts.set(level, (levelCounts.get(level) ?? 0) + 1);
    });

    const levelPositions = new Map<number, number>();

    phases.forEach((phase) => {
      const level = levelMap.get(phase.phase_id) ?? 0;
      const countAtLevel = levelCounts.get(level) ?? 1;
      const currentPos = levelPositions.get(level) ?? 0;
      levelPositions.set(level, currentPos + 1);

      const x = level * (nodeWidth + horizontalGap);
      const totalHeight = countAtLevel * (nodeHeight + verticalGap) - verticalGap;
      const startY = -totalHeight / 2;
      const y = startY + currentPos * (nodeHeight + verticalGap);

      const agentNames = phase.agents
        .map((a) => (typeof a === 'string' ? a : a.agent))
        .join(', ');

      nodeList.push({
        id: phase.phase_id,
        position: { x, y },
        data: {
          label: (
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontWeight: 'bold', marginBottom: 4 }}>{phase.phase_name}</div>
              <div style={{ fontSize: 10, opacity: 0.8 }}>{agentNames}</div>
            </div>
          ),
        },
        style: {
          background: nodeColors[phase.status] || '#9e9e9e',
          color: 'white',
          border: 'none',
          borderRadius: 8,
          padding: 10,
          width: nodeWidth,
          boxShadow: phase.status === 'running' ? '0 0 10px #29B5E8' : undefined,
        },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      });

      if (phase.depends_on) {
        phase.depends_on.forEach((depId) => {
          if (phaseMap.has(depId)) {
            edgeList.push({
              id: `${depId}-${phase.phase_id}`,
              source: depId,
              target: phase.phase_id,
              type: 'smoothstep',
              animated: phase.status === 'running',
              style: {
                stroke: nodeColors[phase.status] || '#9e9e9e',
                strokeWidth: 2,
              },
            });
          }
        });
      }
    });

    return { nodes: nodeList, edges: edgeList };
  }, [phases]);

  if (phases.length === 0) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <span style={{ color: '#9e9e9e' }}>No phases to display</span>
      </div>
    );
  }

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      fitView
      attributionPosition="bottom-left"
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
    >
      <Background color="#333" gap={16} />
      <Controls showInteractive={false} />
      <MiniMap
        nodeColor={(node) => node.style?.background as string || '#9e9e9e'}
        maskColor="rgba(0,0,0,0.8)"
      />
    </ReactFlow>
  );
};

export default PlanViewer;
