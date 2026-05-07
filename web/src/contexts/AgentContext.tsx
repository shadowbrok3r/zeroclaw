import { createContext, useContext, useEffect, useRef, useState, useCallback } from 'react';
import type { WsMessage } from '@/types/api';
import { WebSocketClient, getOrCreateSessionId } from '@/lib/ws';
import { generateUUID } from '@/lib/uuid';
import { t } from '@/lib/i18n';
import { listProps, putProp, getStatus, getSessionMessages, abortSession } from '@/lib/api';
import type { ToolCallInfo } from '@/components/ToolCallCard';
import {
  loadChatHistory,
  mapServerMessagesToPersisted,
  persistedToUiMessages,
  saveChatHistory,
  uiMessagesToPersisted,
} from '@/lib/chatHistoryStorage';

export interface ChatMessage {
  id: string;
  role: 'user' | 'agent';
  content: string;
  thinking?: string;
  markdown?: boolean;
  toolCall?: ToolCallInfo;
  timestamp: Date;
}

/** Supervised tool consent prompt from the gateway WebSocket (`approval_request`). */
export interface AgentApprovalPrompt {
  requestId: string;
  tool: string;
  argumentsSummary: string;
  timeoutSecs: number;
}

interface AgentContextValue {
  messages: ChatMessage[];
  sendMessage: (content: string) => void;
  connected: boolean;
  error: string | null;
  typing: boolean;
  streamingContent: string;
  streamingThinking: string;
  currentModel: string | null;
  availableModels: string[];
  switchModel: (model: string) => Promise<void>;
  modelLoading: boolean;
  /** Re-fetch model list from server. Useful after user edits config externally. */
  refreshModels: () => void;
  deleteMessage: (id: string) => void;
  clearAllMessages: () => void;
  abortSession: () => Promise<void>;
  /** Non-null while the agent waits for `approval_response` on the WebSocket. */
  approvalPrompt: AgentApprovalPrompt | null;
  respondToApproval: (decision: 'approve' | 'deny' | 'always') => void;
}

const AgentContext = createContext<AgentContextValue | null>(null);

export function useAgent() {
  const ctx = useContext(AgentContext);
  if (!ctx) throw new Error('useAgent must be used within AgentProvider');
  return ctx;
}

const MODEL_SWITCH_TIMEOUT_MS = 10_000;

export function AgentProvider({ children }: { children: React.ReactNode }) {
  const sessionIdRef = useRef(getOrCreateSessionId());
  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    const persisted = loadChatHistory(sessionIdRef.current);
    return persisted.length > 0 ? persistedToUiMessages(persisted) : [];
  });
  const [historyReady, setHistoryReady] = useState(false);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [typing, setTyping] = useState(false);
  const [streamingContent, setStreamingContent] = useState('');
  const [streamingThinking, setStreamingThinking] = useState('');
  const [currentModel, setCurrentModel] = useState<string | null>(null);
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [modelLoading, setModelLoading] = useState(false);
  const [modelInfoVersion, setModelInfoVersion] = useState(0);

  const wsRef = useRef<WebSocketClient | null>(null);
  const pendingContentRef = useRef('');
  const pendingThinkingRef = useRef('');
  const capturedThinkingRef = useRef('');
  const pendingModelSwitchRef = useRef<string | null>(null);
  const switchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const wsVersionRef = useRef(0);
  const [approvalPrompt, setApprovalPrompt] = useState<AgentApprovalPrompt | null>(null);
  const approvalPromptRef = useRef<AgentApprovalPrompt | null>(null);

  const clearApprovalPrompt = useCallback(() => {
    approvalPromptRef.current = null;
    setApprovalPrompt(null);
  }, []);

  const respondToApproval = useCallback((decision: 'approve' | 'deny' | 'always') => {
    const pending = approvalPromptRef.current;
    if (!pending) return;
    const ws = wsRef.current;
    if (!ws?.connected) {
      clearApprovalPrompt();
      return;
    }
    try {
      ws.sendApprovalResponse(pending.requestId, decision);
    } catch {
      // Socket closed mid-send — server will treat as deny on timeout.
    }
    clearApprovalPrompt();
  }, [clearApprovalPrompt]);

  // Hydrate chat from server (preferred) or localStorage fallback
  useEffect(() => {
    const sid = sessionIdRef.current;
    let cancelled = false;

    (async () => {
      try {
        const res = await getSessionMessages(sid);
        if (cancelled) return;
        if (res.session_persistence && res.messages.length > 0) {
          setMessages((prev) =>
            prev.length > 0 ? prev : persistedToUiMessages(mapServerMessagesToPersisted(res.messages)),
          );
        } else if (!res.session_persistence) {
          setMessages((prev) => {
            if (prev.length > 0) return prev;
            const ls = loadChatHistory(sid);
            return ls.length ? persistedToUiMessages(ls) : prev;
          });
        }
      } catch {
        if (!cancelled) {
          setMessages((prev) => {
            if (prev.length > 0) return prev;
            const ls = loadChatHistory(sid);
            return ls.length ? persistedToUiMessages(ls) : prev;
          });
        }
      } finally {
        if (!cancelled) setHistoryReady(true);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, []);

  // Mirror transcript to localStorage (bounded); server remains source of truth when persistence is on
  useEffect(() => {
    if (!historyReady) return;
    saveChatHistory(sessionIdRef.current, uiMessagesToPersisted(messages));
  }, [messages, historyReady]);

  // Centralised WebSocket message handler — reused across initial connect and reconnects.
  const handleWsMessage = useCallback((msg: WsMessage) => {
    switch (msg.type) {
      case 'session_start':
      case 'connected':
        break;

      case 'thinking':
        setTyping(true);
        pendingThinkingRef.current += msg.content ?? '';
        setStreamingThinking(pendingThinkingRef.current);
        break;

      case 'chunk':
        setTyping(true);
        pendingContentRef.current += msg.content ?? '';
        setStreamingContent(pendingContentRef.current);
        break;

      case 'chunk_reset':
        // Server signals that the authoritative done message follows.
        // Snapshot thinking before clearing display state.
        capturedThinkingRef.current = pendingThinkingRef.current;
        pendingContentRef.current = '';
        pendingThinkingRef.current = '';
        setStreamingContent('');
        setStreamingThinking('');
        break;

      case 'message':
      case 'done': {
        const content = msg.full_response ?? msg.content ?? pendingContentRef.current;
        const thinking = capturedThinkingRef.current || pendingThinkingRef.current || undefined;
        if (content) {
          setMessages((prev) => [
            ...prev,
            {
              id: generateUUID(),
              role: 'agent',
              content,
              thinking,
              markdown: true,
              timestamp: new Date(),
            },
          ]);
        }
        pendingContentRef.current = '';
        pendingThinkingRef.current = '';
        capturedThinkingRef.current = '';
        setStreamingContent('');
        setStreamingThinking('');
        setTyping(false);
        break;
      }

      case 'tool_call': {
        clearApprovalPrompt();
        const toolName = msg.name ?? 'unknown';
        const toolArgs = msg.args;
        setMessages((prev) => {
          const argsKey = JSON.stringify(toolArgs ?? {});
          if (pendingContentRef.current) {
            const isDuplicate = prev.some(
              (m) => m.toolCall
                && m.toolCall.output === undefined
                && m.toolCall.name === toolName
                && JSON.stringify(m.toolCall.args ?? {}) === argsKey,
            );
            if (isDuplicate) return prev;
          }

          return [
            ...prev,
            {
              id: generateUUID(),
              role: 'agent' as const,
              content: `${t('agent.tool_call_prefix')} ${toolName}(${argsKey})`,
              toolCall: { name: toolName, args: toolArgs },
              timestamp: new Date(),
            },
          ];
        });
        break;
      }

      case 'tool_result': {
        clearApprovalPrompt();
        setMessages((prev) => {
          const idx = prev.findIndex((m) => m.toolCall && m.toolCall.output === undefined);
          if (idx !== -1) {
            const updated = [...prev];
            const existing = prev[idx]!;
            updated[idx] = {
              ...existing,
              toolCall: { ...existing.toolCall!, output: msg.output ?? '' },
            };
            return updated;
          }
          return [
            ...prev,
            {
              id: generateUUID(),
              role: 'agent' as const,
              content: `${t('agent.tool_result_prefix')} ${msg.output ?? ''}`,
              toolCall: { name: msg.name ?? 'unknown', output: msg.output ?? '' },
              timestamp: new Date(),
            },
          ];
        });
        break;
      }

      case 'cron_result': {
        const cronOutput = msg.output ?? '';
        if (cronOutput) {
          setMessages((prev) => [
            ...prev,
            {
              id: generateUUID(),
              role: 'agent' as const,
              content: cronOutput,
              markdown: true,
              timestamp: new Date(msg.timestamp ?? Date.now()),
            },
          ]);
        }
        break;
      }

      case 'error':
        setMessages((prev) => [
          ...prev,
          {
            id: generateUUID(),
            role: 'agent',
            content: `${t('agent.error_prefix')} ${msg.message ?? t('agent.unknown_error')}`,
            timestamp: new Date(),
          },
        ]);
        if (msg.code === 'AGENT_INIT_FAILED' || msg.code === 'AUTH_ERROR' || msg.code === 'PROVIDER_ERROR') {
          setError(`${t('agent.configuration_error')}: ${msg.message}. ${t('agent.check_provider_settings')}.`);
        } else if (msg.code === 'INVALID_JSON' || msg.code === 'UNKNOWN_MESSAGE_TYPE' || msg.code === 'EMPTY_CONTENT') {
          setError(`${t('agent.message_error')}: ${msg.message}`);
        }
        setTyping(false);
        pendingContentRef.current = '';
        pendingThinkingRef.current = '';
        setStreamingContent('');
        setStreamingThinking('');
        clearApprovalPrompt();
        break;

      case 'approval_request': {
        const requestId = msg.request_id;
        const tool = msg.tool;
        if (!requestId || !tool) break;
        const next: AgentApprovalPrompt = {
          requestId,
          tool,
          argumentsSummary: typeof msg.arguments_summary === 'string' ? msg.arguments_summary : '',
          timeoutSecs: typeof msg.timeout_secs === 'number' ? msg.timeout_secs : 120,
        };
        approvalPromptRef.current = next;
        setApprovalPrompt(next);
        break;
      }

      case 'aborted':
        clearApprovalPrompt();
        break;
    }
  }, [clearApprovalPrompt]);

  // Wire up a WebSocketClient instance with version-guarded callbacks.
  const attachSocketCallbacks = useCallback((ws: WebSocketClient) => {
    const version = ++wsVersionRef.current;

    ws.onOpen = () => {
      if (version !== wsVersionRef.current) return;
      setConnected(true);
      setError(null);

      // If we just reconnected after a model switch, apply the pending model now.
      if (pendingModelSwitchRef.current) {
        if (switchTimeoutRef.current) {
          clearTimeout(switchTimeoutRef.current);
          switchTimeoutRef.current = null;
        }
        setCurrentModel(pendingModelSwitchRef.current);
        setModelInfoVersion((v) => v + 1);
        pendingModelSwitchRef.current = null;
        setModelLoading(false);
      }
    };

    ws.onClose = (ev: CloseEvent) => {
      if (version !== wsVersionRef.current) return;
      setConnected(false);
      approvalPromptRef.current = null;
      setApprovalPrompt(null);

      if (pendingModelSwitchRef.current) {
        // We intentionally closed the old socket; non-normal codes mean the reconnect failed.
        if (ev.code !== 1000 && ev.code !== 1001) {
          setError(`${t('agent.connection_closed')} (code: ${ev.code}). ${t('agent.check_configuration')}.`);
        }
        pendingModelSwitchRef.current = null;
        if (switchTimeoutRef.current) {
          clearTimeout(switchTimeoutRef.current);
          switchTimeoutRef.current = null;
        }
        setModelLoading(false);
        return;
      }

      if (ev.code !== 1000 && ev.code !== 1001) {
        setError(`${t('agent.connection_closed')} (code: ${ev.code}). ${t('agent.check_configuration')}.`);
      }
    };

    ws.onError = () => {
      if (version !== wsVersionRef.current) return;
      // During a model switch we let onClose deliver the final verdict.
      if (!pendingModelSwitchRef.current) {
        setError(t('agent.connection_error'));
      }
    };

    ws.onMessage = (msg: WsMessage) => {
      if (version !== wsVersionRef.current) return;
      handleWsMessage(msg);
    };
  }, [handleWsMessage]);

  // Global WebSocket connection — survives route changes.
  useEffect(() => {
    const ws = new WebSocketClient();
    attachSocketCallbacks(ws);
    ws.connect();
    wsRef.current = ws;

    return () => {
      ws.disconnect();
    };
  }, [attachSocketCallbacks]);

  // Fetch current model and available models from config.
  useEffect(() => {
    let cancelled = false;

    async function loadModelInfo() {
      try {
        const status = await getStatus();
        if (cancelled) return;

        let activeModel = status.model;

        // Resolve the concrete model id from providers.models.<fallback>.model when possible.
        try {
          const fb = status.provider;
          if (fb) {
            const list = await listProps(`providers.models.${fb}`);
            const row = list.entries.find((e) => e.path.endsWith('.model'));
            if (row?.populated && typeof row.value === 'string' && row.value.length > 0) {
              activeModel = row.value;
            }
          }
        } catch {
          // ignore
        }
        setCurrentModel(activeModel);

        // Enumerate configured provider models for the dropdown (schema paths use dotted prefixes).
        try {
          const list = await listProps('providers.models');
          const models = list.entries
            .filter((e) => e.path.endsWith('.model') && e.populated && typeof e.value === 'string')
            .map((e) => e.value as string)
            .filter((m) => m.length > 0);
          setAvailableModels(models.length > 0 ? models : [activeModel]);
        } catch {
          setAvailableModels([activeModel]);
        }
      } catch {
        // Ignore errors — dropdown will just show current model once loaded
      }
    }

    loadModelInfo();

    return () => {
      cancelled = true;
    };
  }, [modelInfoVersion]);

  const sendMessage = useCallback((content: string) => {
    if (!wsRef.current?.connected) return;
    try {
      wsRef.current.sendMessage(content);
      setTyping(true);
      pendingContentRef.current = '';
      pendingThinkingRef.current = '';
      setMessages((prev) => [
        ...prev,
        {
          id: generateUUID(),
          role: 'user',
          content,
          timestamp: new Date(),
        },
      ]);
    } catch {
      setError(t('agent.send_error'));
    }
  }, []);

  const switchModel = useCallback(async (model: string) => {
    if (modelLoading) return; // debounce
    setModelLoading(true);
    pendingModelSwitchRef.current = model;

    // Safety net: if the reconnect never succeeds, clear the loading state.
    if (switchTimeoutRef.current) clearTimeout(switchTimeoutRef.current);
    switchTimeoutRef.current = setTimeout(() => {
      if (pendingModelSwitchRef.current) {
        pendingModelSwitchRef.current = null;
        setModelLoading(false);
        setError(t('agent.model_switch_timeout'));
      }
    }, MODEL_SWITCH_TIMEOUT_MS);

    try {
      const statusFresh = await getStatus();
      const profile = statusFresh.provider;
      if (!profile) {
        throw new Error('No providers.fallback — configure a provider first');
      }
      await putProp(`providers.models.${profile}.model`, model);

      // If a turn is actively streaming, abort it on the backend before we tear
      // down the socket. This prevents the old model from continuing to execute
      // tools or persisting its response into the session after we switch.
      if (typing) {
        try {
          await Promise.race([
            abortSession(sessionIdRef.current),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error('abort-timeout')), 1_500),
            ),
          ]);
        } catch {
          // Best-effort: if abort fails or times out we still proceed with the
          // switch so the user is never stuck. The old turn may continue on the
          // server, but the UI will show a clean new session.
        }
      }

      // Abort any in-flight streaming before rebuilding the connection.
      pendingContentRef.current = '';
      pendingThinkingRef.current = '';
      capturedThinkingRef.current = '';
      setStreamingContent('');
      setStreamingThinking('');
      setTyping(false);

      // Tear down the old socket and create a fresh one.
      // The backend will read the updated config when the new socket opens
      // and construct a new Agent with the selected model.
      const oldWs = wsRef.current;
      if (oldWs) {
        oldWs.onOpen = null;
        oldWs.onClose = null;
        oldWs.onError = null;
        oldWs.onMessage = null;
        oldWs.disconnect();
      }

      const ws = new WebSocketClient();
      attachSocketCallbacks(ws);
      ws.connect();
      wsRef.current = ws;
    } catch (err) {
      if (switchTimeoutRef.current) {
        clearTimeout(switchTimeoutRef.current);
        switchTimeoutRef.current = null;
      }
      pendingModelSwitchRef.current = null;
      setModelLoading(false);
      setError(err instanceof Error ? err.message : t('agent.failed_switch_model'));
    }
  }, [attachSocketCallbacks, modelLoading, typing]);

  const deleteMessage = useCallback((id: string) => {
    setMessages((prev) => prev.filter((m) => m.id !== id));
  }, []);

  const clearAllMessages = useCallback(() => {
    setMessages([]);
  }, []);

  const value: AgentContextValue = {
    messages,
    sendMessage,
    connected,
    error,
    typing,
    streamingContent,
    streamingThinking,
    currentModel,
    availableModels,
    switchModel,
    modelLoading,
    refreshModels: () => setModelInfoVersion((v) => v + 1),
    deleteMessage,
    clearAllMessages,
    abortSession: async () => {
      try {
        await abortSession(sessionIdRef.current);
      } catch {
        // Best-effort abort
      }
    },
    approvalPrompt,
    respondToApproval,
  };

  return <AgentContext.Provider value={value}>{children}</AgentContext.Provider>;
}
