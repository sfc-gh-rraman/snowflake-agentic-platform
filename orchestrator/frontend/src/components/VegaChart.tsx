import { useEffect, useRef } from 'react';
import embed from 'vega-embed';

interface VegaChartProps {
  spec: Record<string, unknown>;
  className?: string;
}

export function VegaChart({ spec, className = '' }: VegaChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current || !spec) return;

    const themedSpec = {
      ...spec,
      background: 'transparent',
      config: {
        ...((spec.config as Record<string, unknown>) || {}),
        view: { stroke: 'transparent' },
        axis: {
          domainColor: '#475569',
          gridColor: '#334155',
          tickColor: '#475569',
          labelColor: '#94a3b8',
          titleColor: '#e2e8f0',
        },
        legend: {
          labelColor: '#94a3b8',
          titleColor: '#e2e8f0',
        },
        title: {
          color: '#e2e8f0',
        },
      },
    };

    embed(containerRef.current, themedSpec as never, {
      actions: false,
      theme: 'dark',
      renderer: 'svg',
    }).catch(console.error);

    return () => {
      if (containerRef.current) {
        containerRef.current.innerHTML = '';
      }
    };
  }, [spec]);

  return (
    <div
      ref={containerRef}
      className={`vega-chart bg-slate-900/50 rounded-lg p-4 ${className}`}
    />
  );
}
