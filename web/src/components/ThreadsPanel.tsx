import { useCallback, useEffect, useMemo, useState } from 'react';
import { Check, Hash, MessageSquare, Pencil, Plus, RefreshCw, Trash2, X } from 'lucide-react';
import type { Session, SessionLifecycleEvent, SessionMessageRow } from '@/types/api';
import { getSessions, getSessionMessages, deleteSession, renameSession } from '@/lib/api';
import { useAgent } from '@/contexts/AgentContext';
import { useSSE } from '@/hooks/useSSE';
import { formatRelative } from '@/lib/format';
import { t } from '@/lib/i18n';
import { Button, ConfirmDialog } from '@/components/ui';

/** Shortened display form for an unnamed thread (first uuid block). */
function shortThreadId(id: string): string {
  return id.length > 8 ? id.slice(0, 8) : id;
}

interface TranscriptViewer {
  session: Session;
  messages: SessionMessageRow[] | null;
  error: string | null;
}

export interface ThreadsPanelProps {
  agentAlias: string;
  onClose: () => void;
}

/**
 * Per-agent session browser, opened from the chat header. Two sections:
 *
 * - **Chat threads** (`channel_id` null — gateway WebSocket sessions): open
 *   (switch the live chat onto that thread), rename, and delete; plus a
 *   "new thread" action. The current thread is highlighted.
 * - **Channel conversations** (`channel_id` set — Discord etc.): read-only;
 *   clicking a row opens a transcript viewer via the session messages API.
 *
 * The list refetches on mount (the panel is mounted only while open) and
 * refreshes live on `session_created` / `session_update` / `session_closed`
 * SSE frames. Rendered inside an `<AgentProvider>` (uses `useAgent()`).
 */
export default function ThreadsPanel({ agentAlias, onClose }: ThreadsPanelProps) {
  const { sessionId, startNewThread, switchThread } = useAgent();

  const [sessions, setSessions] = useState<Session[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  // session_key currently in inline-rename mode, with its draft value.
  const [renaming, setRenaming] = useState<{ key: string; value: string } | null>(null);
  const [renameSaving, setRenameSaving] = useState(false);
  const [pendingDelete, setPendingDelete] = useState<Session | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [viewer, setViewer] = useState<TranscriptViewer | null>(null);

  const load = useCallback(() => {
    getSessions()
      .then((rows) => {
        setSessions(rows.filter((r) => r.agent_alias === agentAlias));
        setError(null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [agentAlias]);

  useEffect(() => {
    load();
  }, [load]);

  // Live refresh on session lifecycle frames. The panel is mounted only while
  // open, so this SSE subscription exists only while the user is looking.
  const { events } = useSSE({
    filterTypes: ['session_created', 'session_update', 'session_closed'],
    autoConnect: true,
  });
  useEffect(() => {
    if (events.length === 0) return;
    const last = events[events.length - 1] as Partial<SessionLifecycleEvent> | undefined;
    // Skip frames attributed to a different agent; refresh on unattributed ones.
    if (last?.agent_alias && last.agent_alias !== agentAlias) return;
    load();
  }, [events.length, events, agentAlias, load]);

  const chatThreads = useMemo(
    () =>
      (sessions ?? [])
        .filter((s) => !s.channel_id)
        .sort((a, b) => b.last_activity.localeCompare(a.last_activity)),
    [sessions],
  );
  const channelConversations = useMemo(
    () =>
      (sessions ?? [])
        .filter((s) => !!s.channel_id)
        .sort((a, b) => b.last_activity.localeCompare(a.last_activity)),
    [sessions],
  );

  const handleOpenThread = (s: Session) => {
    // The display `session_id` (gw_ stripped) is the raw uuid the WebSocket
    // connects with; switchThread persists it and reconnects.
    if (s.session_id !== sessionId) switchThread(s.session_id);
    onClose();
  };

  const commitRename = async () => {
    if (!renaming || renameSaving) return;
    const { key, value } = renaming;
    setRenameSaving(true);
    try {
      const trimmed = value.trim();
      await renameSession(key, trimmed);
      setSessions((prev) =>
        prev
          ? prev.map((s) =>
              s.session_key === key ? { ...s, name: trimmed || undefined } : s,
            )
          : prev,
      );
      setRenaming(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRenameSaving(false);
    }
  };

  const handleDelete = async (s: Session) => {
    if (deleting) return;
    setDeleting(s.session_key);
    try {
      await deleteSession(s.session_key);
      setSessions((prev) =>
        prev ? prev.filter((row) => row.session_key !== s.session_key) : prev,
      );
      // Deleting the thread the chat is currently on: move onto a fresh one so
      // the next turn doesn't resurrect the just-deleted session key.
      if (s.session_id === sessionId) startNewThread();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setDeleting(null);
    }
  };

  const openViewer = (s: Session) => {
    setViewer({ session: s, messages: null, error: null });
    getSessionMessages(s.session_key)
      .then((resp) =>
        setViewer((curr) =>
          curr && curr.session.session_key === s.session_key
            ? { ...curr, messages: resp.messages }
            : curr,
        ),
      )
      .catch((e) =>
        setViewer((curr) =>
          curr && curr.session.session_key === s.session_key
            ? { ...curr, error: e instanceof Error ? e.message : String(e) }
            : curr,
        ),
      );
  };

  return (
    <>
    <div
      className="fixed inset-0 z-50 flex justify-end animate-fade-in"
      style={{ background: 'rgba(0,0,0,0.5)' }}
      onClick={onClose}
    >
      <div
        className="h-full w-full max-w-md flex flex-col border-l"
        style={{ background: 'var(--pc-bg-surface)', borderColor: 'var(--pc-border)' }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          className="flex items-center gap-2 px-4 py-3 border-b"
          style={{ borderColor: 'var(--pc-border)' }}
        >
          <MessageSquare className="h-4 w-4" style={{ color: 'var(--pc-accent)' }} />
          <h2 className="text-sm font-semibold" style={{ color: 'var(--pc-text-primary)' }}>
            {t('threads.title')}
          </h2>
          <span className="text-xs" style={{ color: 'var(--pc-text-faint)' }}>
            {agentAlias}
          </span>
          <div className="ml-auto flex items-center gap-1">
            <button
              type="button"
              onClick={load}
              className="p-1.5 rounded-lg hover:bg-[var(--pc-hover)]"
              title={t('common.refresh')}
              style={{ color: 'var(--pc-text-muted)' }}
            >
              <RefreshCw className="h-4 w-4" />
            </button>
            <Button
              variant="primary"
              size="sm"
              onClick={() => {
                startNewThread();
                onClose();
              }}
            >
              <Plus className="h-3.5 w-3.5" />
              {t('threads.new_thread')}
            </Button>
            <button
              type="button"
              onClick={onClose}
              className="p-1.5 rounded-lg hover:bg-[var(--pc-hover)]"
              title={t('common.close')}
              style={{ color: 'var(--pc-text-muted)' }}
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-4 space-y-5">
          {error && (
            <p className="text-sm" style={{ color: 'var(--color-status-error)' }}>
              {t('threads.load_error')}: {error}
            </p>
          )}
          {sessions === null && !error && (
            <p className="text-sm" style={{ color: 'var(--pc-text-muted)' }}>
              {t('threads.loading')}
            </p>
          )}

          {sessions !== null && (
            <>
              {/* Chat threads */}
              <section>
                <h3
                  className="text-xs font-semibold uppercase tracking-wider mb-2"
                  style={{ color: 'var(--pc-text-muted)' }}
                >
                  {t('threads.chat_threads')}
                </h3>
                {chatThreads.length === 0 ? (
                  <p className="text-sm py-2" style={{ color: 'var(--pc-text-faint)' }}>
                    {t('threads.none')}
                  </p>
                ) : (
                  <div className="space-y-2">
                    {chatThreads.map((s) => {
                      const isCurrent = s.session_id === sessionId;
                      const isRenaming = renaming?.key === s.session_key;
                      return (
                        <div
                          key={s.session_key}
                          className="flex items-center gap-2 py-2 px-3 rounded-xl"
                          style={{
                            background: 'var(--pc-bg-elevated)',
                            border: isCurrent
                              ? '1px solid var(--pc-accent)'
                              : '1px solid transparent',
                          }}
                        >
                          {isRenaming ? (
                            <div className="flex-1 min-w-0">
                              <input
                                autoFocus
                                value={renaming.value}
                                onChange={(e) =>
                                  setRenaming({ key: s.session_key, value: e.target.value })
                                }
                                onKeyDown={(e) => {
                                  if (e.key === 'Enter') void commitRename();
                                  if (e.key === 'Escape') setRenaming(null);
                                }}
                                placeholder={t('threads.rename_placeholder')}
                                className="input-electric px-2 py-0.5 text-xs w-40"
                                aria-label={t('threads.rename')}
                              />
                            </div>
                          ) : (
                          <button
                            type="button"
                            onClick={() => handleOpenThread(s)}
                            className="flex-1 min-w-0 text-left"
                            title={t('threads.open_thread')}
                          >
                            <span className="flex items-center gap-2 flex-wrap">
                              <span
                                className={`text-sm font-medium truncate ${s.name ? '' : 'font-mono'}`}
                                style={{ color: 'var(--pc-text-primary)' }}
                              >
                                {s.name || shortThreadId(s.session_id)}
                              </span>
                              {isCurrent && (
                                <span
                                  className="text-[10px] font-medium px-2 py-0.5 rounded-full"
                                  style={{
                                    background: 'rgba(var(--pc-accent-rgb), 0.10)',
                                    color: 'var(--pc-accent)',
                                  }}
                                >
                                  {t('threads.current')}
                                </span>
                              )}
                            </span>
                            <span
                              className="flex items-center gap-2 text-xs mt-0.5"
                              style={{ color: 'var(--pc-text-muted)' }}
                            >
                              <span className="flex items-center gap-1">
                                <MessageSquare className="h-3 w-3" />
                                {s.message_count}
                              </span>
                              <span>{formatRelative(s.last_activity)}</span>
                            </span>
                          </button>
                          )}
                          <div className="flex items-center gap-0.5 flex-shrink-0">
                            {isRenaming ? (
                              <>
                                <button
                                  type="button"
                                  onClick={() => void commitRename()}
                                  disabled={renameSaving}
                                  className="p-1.5 rounded-lg hover:bg-[var(--pc-hover)] disabled:opacity-50"
                                  title={t('common.save')}
                                  style={{ color: 'var(--color-status-success)' }}
                                >
                                  <Check className="h-4 w-4" />
                                </button>
                                <button
                                  type="button"
                                  onClick={() => setRenaming(null)}
                                  className="p-1.5 rounded-lg hover:bg-[var(--pc-hover)]"
                                  title={t('common.cancel')}
                                  style={{ color: 'var(--pc-text-muted)' }}
                                >
                                  <X className="h-4 w-4" />
                                </button>
                              </>
                            ) : (
                              <>
                                <button
                                  type="button"
                                  onClick={() =>
                                    setRenaming({ key: s.session_key, value: s.name ?? '' })
                                  }
                                  className="p-1.5 rounded-lg hover:bg-[var(--pc-hover)]"
                                  title={t('threads.rename')}
                                  style={{ color: 'var(--pc-text-muted)' }}
                                >
                                  <Pencil className="h-4 w-4" />
                                </button>
                                <button
                                  type="button"
                                  onClick={() => setPendingDelete(s)}
                                  disabled={deleting === s.session_key}
                                  className="p-1.5 rounded-lg hover:bg-[var(--pc-hover)] disabled:opacity-50"
                                  title={t('threads.delete')}
                                  style={{ color: 'var(--color-status-error)' }}
                                >
                                  <Trash2 className="h-4 w-4" />
                                </button>
                              </>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </section>

              {/* Channel conversations (read-only) */}
              <section>
                <h3
                  className="text-xs font-semibold uppercase tracking-wider mb-2"
                  style={{ color: 'var(--pc-text-muted)' }}
                >
                  {t('threads.channel_conversations')}
                </h3>
                {channelConversations.length === 0 ? (
                  <p className="text-sm py-2" style={{ color: 'var(--pc-text-faint)' }}>
                    {t('threads.no_channel_conversations')}
                  </p>
                ) : (
                  <div className="space-y-2">
                    {channelConversations.map((s) => (
                      <button
                        key={s.session_key}
                        type="button"
                        onClick={() => openViewer(s)}
                        className="w-full text-left flex items-center gap-2 py-2 px-3 rounded-xl hover:bg-[var(--pc-hover)]"
                        style={{
                          background: 'var(--pc-bg-elevated)',
                          border: '1px solid transparent',
                        }}
                        title={t('threads.view_transcript')}
                      >
                        <div className="flex-1 min-w-0">
                          <span className="flex items-center gap-2 flex-wrap">
                            <span
                              className="text-sm font-mono truncate"
                              style={{ color: 'var(--pc-text-primary)' }}
                            >
                              {s.name || s.session_id}
                            </span>
                            <span
                              className="text-[10px] font-mono px-2 py-0.5 rounded-full flex items-center gap-1"
                              style={{ background: 'rgba(167, 139, 250, 0.10)', color: '#a78bfa' }}
                            >
                              <Hash className="h-2.5 w-2.5" />
                              {s.channel_id}
                            </span>
                          </span>
                          <span
                            className="flex items-center gap-2 text-xs mt-0.5"
                            style={{ color: 'var(--pc-text-muted)' }}
                          >
                            <span className="flex items-center gap-1">
                              <MessageSquare className="h-3 w-3" />
                              {s.message_count}
                            </span>
                            <span>{formatRelative(s.last_activity)}</span>
                            <span style={{ color: 'var(--pc-text-faint)' }}>
                              {t('threads.read_only')}
                            </span>
                          </span>
                        </div>
                      </button>
                    ))}
                  </div>
                )}
              </section>
            </>
          )}
        </div>
      </div>
    </div>

      {/* Read-only transcript viewer for channel conversations. Rendered as a
          sibling of the panel backdrop so its clicks never bubble into the
          panel's close handler. */}
      {viewer && (
        <div
          className="fixed inset-0 z-[60] flex items-center justify-center p-4"
          style={{ background: 'rgba(0,0,0,0.5)' }}
          onClick={() => setViewer(null)}
        >
          <div
            className="card p-5 w-full max-w-2xl max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start justify-between mb-4 gap-3">
              <div className="min-w-0">
                <p
                  className="text-xs uppercase tracking-wider mb-1"
                  style={{ color: 'var(--pc-text-faint)' }}
                >
                  {t('threads.transcript')}
                </p>
                <p
                  className="text-sm font-mono break-all"
                  style={{ color: 'var(--pc-text-primary)' }}
                >
                  {viewer.session.name || viewer.session.session_id}
                </p>
                {viewer.session.channel_id && (
                  <p className="text-xs font-mono mt-1" style={{ color: '#a78bfa' }}>
                    {viewer.session.channel_id}
                  </p>
                )}
              </div>
              <button
                type="button"
                onClick={() => setViewer(null)}
                className="p-1 rounded-lg hover:bg-[var(--pc-hover)] flex-shrink-0"
                style={{ color: 'var(--pc-text-muted)' }}
                title={t('common.close')}
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto space-y-3 pr-1">
              {viewer.error ? (
                <p className="text-sm" style={{ color: 'var(--color-status-error)' }}>
                  {viewer.error}
                </p>
              ) : viewer.messages === null ? (
                <p className="text-sm" style={{ color: 'var(--pc-text-muted)' }}>
                  {t('dashboard.loading_transcript')}
                </p>
              ) : viewer.messages.length === 0 ? (
                <p className="text-sm" style={{ color: 'var(--pc-text-faint)' }}>
                  {t('dashboard.no_persisted_messages')}
                </p>
              ) : (
                viewer.messages.map((m, i) => (
                  <div
                    key={i}
                    className="rounded-xl px-3 py-2"
                    style={{ background: 'var(--pc-bg-elevated)' }}
                  >
                    <p
                      className="text-[10px] uppercase tracking-wider font-mono mb-1"
                      style={{ color: 'var(--pc-text-faint)' }}
                    >
                      {m.role}
                    </p>
                    <p
                      className="text-sm whitespace-pre-wrap break-words"
                      style={{ color: 'var(--pc-text-primary)' }}
                    >
                      {m.content}
                    </p>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}

      <ConfirmDialog
        open={pendingDelete !== null}
        danger
        title={t('threads.delete')}
        message={`${t('threads.confirm_delete_prefix')} ${
          pendingDelete?.name || shortThreadId(pendingDelete?.session_id ?? '')
        }${t('threads.confirm_delete_suffix')}`}
        confirmLabel={t('common.delete')}
        onConfirm={() => {
          const target = pendingDelete;
          setPendingDelete(null);
          if (target) void handleDelete(target);
        }}
        onClose={() => setPendingDelete(null)}
      />
    </>
  );
}
