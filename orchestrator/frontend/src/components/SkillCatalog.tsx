import { useEffect, useState } from 'react';
import {
  GitBranch,
  ExternalLink,
  Loader2,
  Search,
  Database,
  Shield,
  FileText,
  Brain,
  Activity,
  Sparkles,
  ChevronDown,
  ChevronRight,
  Package,
  AlertCircle,
} from 'lucide-react';

interface Skill {
  path: string;
  folder: string;
  name: string;
  description: string;
  category: string;
  triggers: string;
  repo: string;
  url: string;
}

interface CatalogResponse {
  source: string;
  repo: string;
  branch?: string;
  skills: Skill[];
  error?: string;
}

const CATEGORY_ICONS: Record<string, typeof Database> = {
  provider: Database,
  pharma: Shield,
  'cross-industry': Activity,
  clinical: FileText,
  drug: Shield,
  ml: Brain,
};

function getCategoryIcon(category: string) {
  const lower = category.toLowerCase();
  for (const [key, Icon] of Object.entries(CATEGORY_ICONS)) {
    if (lower.includes(key)) return Icon;
  }
  return Sparkles;
}

const CATEGORY_COLORS: Record<string, string> = {
  provider: 'bg-cyan-500/10 border-cyan-500/30 text-cyan-400',
  pharma: 'bg-amber-500/10 border-amber-500/30 text-amber-400',
  'cross-industry': 'bg-violet-500/10 border-violet-500/30 text-violet-400',
};

function getCategoryColor(category: string) {
  const lower = category.toLowerCase();
  for (const [key, cls] of Object.entries(CATEGORY_COLORS)) {
    if (lower.includes(key)) return cls;
  }
  return 'bg-slate-500/10 border-slate-500/30 text-slate-400';
}

export function SkillCatalog() {
  const [catalog, setCatalog] = useState<CatalogResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());

  useEffect(() => {
    setLoading(true);
    fetch('/api/skills/catalog')
      .then((res) => res.json())
      .then((data) => {
        setCatalog(data);
        const cats = new Set(data.skills?.map((s: Skill) => s.category) || []);
        setExpandedCategories(cats as Set<string>);
      })
      .catch((e) => setCatalog({ source: 'error', repo: '', skills: [], error: String(e) }))
      .finally(() => setLoading(false));
  }, []);

  const toggleCategory = (cat: string) => {
    setExpandedCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-slate-400">
        <Loader2 className="w-5 h-5 animate-spin mr-2" />
        <span className="text-sm">Loading skills catalog from GitHub...</span>
      </div>
    );
  }

  const skills = catalog?.skills || [];
  const filtered = skills.filter(
    (s) =>
      !searchTerm ||
      s.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.triggers.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.category.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const grouped = filtered.reduce<Record<string, Skill[]>>((acc, s) => {
    const cat = s.category || 'Uncategorized';
    if (!acc[cat]) acc[cat] = [];
    acc[cat].push(s);
    return acc;
  }, {});

  return (
    <div className="h-full flex flex-col bg-slate-900">
      <div className="px-4 py-3 border-b border-slate-800">
        <div className="flex items-center gap-2 mb-2">
          <Package className="w-4 h-4 text-cyan-400" />
          <span className="text-sm font-semibold text-white">Skills Catalog</span>
          <span className="text-[10px] bg-slate-800 text-slate-400 px-1.5 py-0.5 rounded-full">
            {skills.length} skills
          </span>
        </div>
        {catalog?.repo && (
          <div className="flex items-center gap-1.5 mb-2">
            <GitBranch className="w-3 h-3 text-slate-500" />
            <a
              href={`https://github.com/${catalog.repo}`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-[10px] text-slate-500 hover:text-cyan-400 transition-colors"
            >
              {catalog.repo}
            </a>
            {catalog.source === 'error' && (
              <span className="flex items-center gap-1 text-[10px] text-amber-400">
                <AlertCircle className="w-3 h-3" />
                fallback catalog
              </span>
            )}
          </div>
        )}
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500" />
          <input
            type="text"
            placeholder="Search skills..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-8 pr-3 py-1.5 bg-slate-800 border border-slate-700 rounded-lg text-xs text-white placeholder-slate-500 focus:outline-none focus:border-cyan-600"
          />
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-4 py-2">
        {Object.entries(grouped).map(([category, catSkills]) => {
          const Icon = getCategoryIcon(category);
          const isExpanded = expandedCategories.has(category);

          return (
            <div key={category} className="mb-3">
              <button
                onClick={() => toggleCategory(category)}
                className="flex items-center gap-2 w-full text-left py-1.5 hover:bg-slate-800/50 rounded px-2 -mx-2 transition-colors"
              >
                {isExpanded ? (
                  <ChevronDown className="w-3 h-3 text-slate-500" />
                ) : (
                  <ChevronRight className="w-3 h-3 text-slate-500" />
                )}
                <Icon className="w-3.5 h-3.5 text-slate-400" />
                <span className="text-xs font-medium text-slate-300">{category}</span>
                <span className="text-[10px] text-slate-600">{catSkills.length}</span>
              </button>

              {isExpanded && (
                <div className="ml-5 mt-1 space-y-1.5">
                  {catSkills.map((skill) => {
                    const catColor = getCategoryColor(category);
                    return (
                      <a
                        key={skill.path}
                        href={skill.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="block rounded-lg border border-slate-800 bg-slate-800/40 p-3 hover:bg-slate-800/80 hover:border-slate-700 transition-all group"
                      >
                        <div className="flex items-start justify-between mb-1">
                          <span className="text-xs font-medium text-white group-hover:text-cyan-300 transition-colors">
                            {skill.name}
                          </span>
                          <ExternalLink className="w-3 h-3 text-slate-600 group-hover:text-cyan-400 transition-colors flex-shrink-0 mt-0.5" />
                        </div>
                        <p className="text-[10px] text-slate-400 mb-2 line-clamp-2">
                          {skill.description}
                        </p>
                        {skill.triggers && (
                          <div className="flex flex-wrap gap-1">
                            {skill.triggers.split(',').slice(0, 4).map((trigger, i) => (
                              <span
                                key={i}
                                className={`text-[9px] px-1.5 py-0.5 rounded-full border ${catColor}`}
                              >
                                {trigger.trim()}
                              </span>
                            ))}
                          </div>
                        )}
                      </a>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
        {filtered.length === 0 && (
          <div className="text-center text-slate-500 text-xs py-8">
            No skills found matching "{searchTerm}"
          </div>
        )}
      </div>
    </div>
  );
}
