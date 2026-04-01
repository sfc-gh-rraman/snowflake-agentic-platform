import { useWorkflowStore } from '../stores/workflowStore';
import { Users, Shield, Eye, EyeOff, Lock } from 'lucide-react';

const ROLES = [
  {
    name: 'ACCOUNTADMIN',
    label: 'Account Admin',
    color: 'text-red-400',
    bgColor: 'bg-red-900/20 border-red-800',
    access: { execute: true, viewLogs: true, viewCosts: true, viewGovernance: true, chat: true, export: true, viewAllSchemas: true },
    description: 'Full access to all orchestrator features and data',
  },
  {
    name: 'DATA_ENGINEER',
    label: 'Data Engineer',
    color: 'text-cyan-400',
    bgColor: 'bg-cyan-900/20 border-cyan-800',
    access: { execute: true, viewLogs: true, viewCosts: false, viewGovernance: false, chat: true, export: true, viewAllSchemas: true },
    description: 'Can execute pipelines and view logs, no cost/governance',
  },
  {
    name: 'DATA_SCIENTIST',
    label: 'Data Scientist',
    color: 'text-purple-400',
    bgColor: 'bg-purple-900/20 border-purple-800',
    access: { execute: false, viewLogs: true, viewCosts: false, viewGovernance: false, chat: true, export: false, viewAllSchemas: false },
    description: 'Read-only access to ML/Analytics schemas, can chat with data',
  },
  {
    name: 'COMPLIANCE_OFFICER',
    label: 'Compliance Officer',
    color: 'text-amber-400',
    bgColor: 'bg-amber-900/20 border-amber-800',
    access: { execute: false, viewLogs: true, viewCosts: true, viewGovernance: true, chat: false, export: true, viewAllSchemas: false },
    description: 'View governance, costs, and audit logs only',
  },
  {
    name: 'VIEWER',
    label: 'Read-Only Viewer',
    color: 'text-slate-400',
    bgColor: 'bg-slate-800/50 border-slate-700',
    access: { execute: false, viewLogs: true, viewCosts: false, viewGovernance: false, chat: false, export: false, viewAllSchemas: false },
    description: 'View workflow status and logs only',
  },
];

const ACCESS_LABELS: Record<string, string> = {
  execute: 'Execute Pipelines',
  viewLogs: 'View Execution Logs',
  viewCosts: 'View Cost Data',
  viewGovernance: 'View Governance',
  chat: 'Cortex AI Chat',
  export: 'Export Templates',
  viewAllSchemas: 'All Schema Access',
};

export function RoleBasedView() {
  const { selectedRole, setSelectedRole } = useWorkflowStore();

  const currentRole = ROLES.find((r) => r.name === selectedRole) || ROLES[0];

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center gap-2">
        <Users className="w-4 h-4 text-cyan-400" />
        <span className="text-sm font-semibold text-white">Role-Based Access View</span>
      </div>

      <div className="space-y-1.5">
        {ROLES.map((role) => (
          <button
            key={role.name}
            onClick={() => setSelectedRole(role.name)}
            className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg border text-left transition-all ${
              selectedRole === role.name
                ? `${role.bgColor} ring-1 ring-white/10`
                : 'bg-slate-800/30 border-slate-800 hover:border-slate-700'
            }`}
          >
            <Shield className={`w-4 h-4 ${role.color} shrink-0`} />
            <div className="flex-1 min-w-0">
              <span className={`text-xs font-medium ${role.color}`}>{role.label}</span>
              <p className="text-[10px] text-slate-500 truncate">{role.description}</p>
            </div>
            {selectedRole === role.name && (
              <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-white/10 text-white">Active</span>
            )}
          </button>
        ))}
      </div>

      <div className="border-t border-slate-800 pt-3">
        <span className="text-xs text-slate-500 mb-2 block">
          Access Matrix for {currentRole.label}
        </span>
        <div className="space-y-1">
          {Object.entries(currentRole.access).map(([key, allowed]) => (
            <div key={key} className="flex items-center justify-between px-2 py-1">
              <span className="text-xs text-slate-400">{ACCESS_LABELS[key] || key}</span>
              {allowed ? (
                <div className="flex items-center gap-1">
                  <Eye className="w-3 h-3 text-green-400" />
                  <span className="text-[10px] text-green-400">Allowed</span>
                </div>
              ) : (
                <div className="flex items-center gap-1">
                  <EyeOff className="w-3 h-3 text-red-400" />
                  <span className="text-[10px] text-red-400">Denied</span>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      <div className="bg-slate-800/30 rounded-lg p-3 border border-slate-700/50">
        <div className="flex items-center gap-1.5 mb-1">
          <Lock className="w-3 h-3 text-amber-400" />
          <span className="text-[10px] text-amber-400 font-medium">RBAC Note</span>
        </div>
        <p className="text-[10px] text-slate-500">
          Role simulation shows how the dashboard appears under different Snowflake roles.
          Actual enforcement requires Snowflake RBAC grants.
        </p>
      </div>
    </div>
  );
}

export function getRoleAccess(selectedRole: string) {
  const role = ROLES.find((r) => r.name === selectedRole);
  return role?.access || ROLES[0].access;
}
