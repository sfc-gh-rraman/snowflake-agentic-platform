import { useEffect, useRef } from 'react';
import { useWorkflowStore } from '../stores/workflowStore';
import { WebSocketMessage } from '../types/workflow';

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const { addLog } = useWorkflowStore();

  useEffect(() => {
    const connect = () => {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${window.location.host}/ws`;
      
      try {
        wsRef.current = new WebSocket(wsUrl);
        
        wsRef.current.onopen = () => {
          console.log('[WS] Connected');
        };
        
        wsRef.current.onmessage = (event) => {
          try {
            const message: WebSocketMessage = JSON.parse(event.data);
            handleMessage(message);
          } catch (e) {
            console.error('[WS] Parse error:', e);
          }
        };
        
        wsRef.current.onclose = () => {
          console.log('[WS] Disconnected, reconnecting...');
          reconnectTimeoutRef.current = window.setTimeout(connect, 3000);
        };
        
        wsRef.current.onerror = (error) => {
          console.error('[WS] Error:', error);
        };
      } catch (e) {
        console.error('[WS] Connection error:', e);
        reconnectTimeoutRef.current = window.setTimeout(connect, 3000);
      }
    };
    
    const handleMessage = (message: WebSocketMessage) => {
      switch (message.type) {
        case 'task_log':
          addLog({
            timestamp: message.timestamp || new Date().toISOString(),
            taskId: message.payload.taskId as string,
            level: message.payload.level as 'info' | 'success' | 'error' | 'warning',
            message: message.payload.message as string,
          });
          break;
        case 'pong':
          break;
        default:
          console.log('[WS] Message:', message);
      }
    };
    
    connect();
    
    const pingInterval = setInterval(() => {
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        wsRef.current.send('ping');
      }
    }, 30000);
    
    return () => {
      clearInterval(pingInterval);
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      wsRef.current?.close();
    };
  }, [addLog]);
  
  return wsRef;
}
