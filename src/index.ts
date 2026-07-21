import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema, ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import express, { type Request, type Response, type NextFunction } from 'express';
import { createHmac, randomInt } from 'node:crypto';
import { WhoopClient } from './whoop-client.js';
import { WhoopDatabase } from './database.js';
import { WhoopSync } from './sync.js';
import { sendLoginCodeEmail } from './email.js';

interface ToolArguments {
  days?: number;
  full?: boolean;
}

const config = {
  clientId: process.env.WHOOP_CLIENT_ID ?? '',
  clientSecret: process.env.WHOOP_CLIENT_SECRET ?? '',
  redirectUri: process.env.WHOOP_REDIRECT_URI ?? 'http://localhost:3000/callback',
  dbPath: process.env.DB_PATH ?? './whoop.db',
  port: Number.parseInt(process.env.PORT ?? '3000', 10),
  mode: process.env.MCP_MODE ?? 'http',
};

const db = new WhoopDatabase(config.dbPath);

// Legacy single-owner client/sync, used only by the MCP tool handlers below
// (Claude Desktop etc.) and left completely untouched by the multi-tenant work.
const client = new WhoopClient({
  clientId: config.clientId,
  clientSecret: config.clientSecret,
  redirectUri: config.redirectUri,
  onTokenRefresh: tokens => db.saveTokens(tokens),
});

const existingTokens = db.getTokens();
if (existingTokens) {
  client.setTokens(existingTokens);
}

const sync = new WhoopSync(client, db);

const SESSION_TTL_MS = 30 * 60 * 1000;
const transports = new Map<string, { transport: StreamableHTTPServerTransport; lastAccess: number }>();

function cleanupStaleSessions(): void {
  const now = Date.now();
  for (const [sessionId, session] of transports) {
    if (now - session.lastAccess > SESSION_TTL_MS) {
      session.transport.close().catch(() => {});
      transports.delete(sessionId);
    }
  }
}

setInterval(cleanupStaleSessions, 5 * 60 * 1000);

function formatDuration(millis: number | null): string {
  if (!millis) return 'N/A';
  const hours = Math.floor(millis / 3_600_000);
  const minutes = Math.floor((millis % 3_600_000) / 60_000);
  return `${hours}h ${minutes}m`;
}

function formatDate(isoString: string): string {
  return new Date(isoString).toLocaleDateString('en-US', {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
  });
}

function getRecoveryZone(score: number): string {
  if (score >= 67) return 'Green (Well Recovered)';
  if (score >= 34) return 'Yellow (Moderate)';
  return 'Red (Needs Rest)';
}

function getStrainZone(strain: number): string {
  if (strain >= 18) return 'All Out (18-21)';
  if (strain >= 14) return 'High (14-17)';
  if (strain >= 10) return 'Moderate (10-13)';
  return 'Light (0-9)';
}

const SPORT_NAMES: Record<number, string> = {
  0: 'Running', 1: 'Cycling', 18: 'Rowing', 33: 'Swimming', 39: 'Boxing',
  44: 'Yoga', 45: 'Weightlifting', 48: 'Functional Fitness', 52: 'Hiking/Rucking',
  56: 'Martial Arts', 59: 'Powerlifting', 63: 'Walking', 65: 'Elliptical',
  84: 'Jumping Rope', 96: 'HIIT', 97: 'Spin', 123: 'Strength Trainer',
  126: 'Assault Bike', 128: 'Stretching', '-1': 'Activity',
};

function getSportName(sportId: number): string {
  return SPORT_NAMES[sportId] ?? `Sport #${sportId}`;
}

function validateDays(value: unknown): number {
  if (value === undefined || value === null) return 14;
  const num = typeof value === 'number' ? value : Number.parseInt(String(value), 10);
  if (Number.isNaN(num) || num < 1) return 14;
  return Math.min(num, 90);
}

function validateBoolean(value: unknown): boolean {
  if (typeof value === 'boolean') return value;
  if (value === 'true') return true;
  return false;
}

function createMcpServer(): Server {
  const server = new Server(
    { name: 'whoop-mcp-server', version: '1.0.0' },
    { capabilities: { tools: {} } }
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      {
        name: 'get_today',
        description: "Get today's Whoop data including recovery score, last night's sleep, and current strain.",
        inputSchema: { type: 'object', properties: {}, required: [] },
      },
      {
        name: 'get_recovery_trends',
        description: 'Get recovery score trends over time, including HRV and resting heart rate patterns.',
        inputSchema: {
          type: 'object',
          properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 90)' } },
          required: [],
        },
      },
      {
        name: 'get_sleep_analysis',
        description: 'Get detailed sleep analysis including duration, stages, efficiency, and sleep debt.',
        inputSchema: {
          type: 'object',
          properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 90)' } },
          required: [],
        },
      },
      {
        name: 'get_strain_history',
        description: 'Get training strain history and workout data.',
        inputSchema: {
          type: 'object',
          properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 90)' } },
          required: [],
        },
      },
      {
        name: 'sync_data',
        description: 'Manually trigger a data sync from Whoop.',
        inputSchema: {
          type: 'object',
          properties: { full: { type: 'boolean', description: 'Force a full 90-day sync (default: false)' } },
          required: [],
        },
      },
      {
        name: 'get_workouts',
        description: 'Get individual workout sessions (sport type, duration, avg/max heart rate, calories, strain) for a date range. Useful for strength training sessions where the daily strain total alone is misleading.',
        inputSchema: {
          type: 'object',
          properties: { days: { type: 'number', description: 'Number of days to look back (default: 14, max: 90)' } },
          required: [],
        },
      },
      {
        name: 'get_auth_url',
        description: 'Get the Whoop authorization URL to connect your account.',
        inputSchema: { type: 'object', properties: {}, required: [] },
      },
    ],
  }));

  server.setRequestHandler(CallToolRequestSchema, async request => {
    const { name, arguments: args } = request.params;
    const typedArgs = (args ?? {}) as ToolArguments;

    try {
      const dataTools = ['get_today', 'get_recovery_trends', 'get_sleep_analysis', 'get_strain_history', 'get_workouts'];
      if (dataTools.includes(name)) {
        const tokens = db.getTokens();
        if (!tokens) {
          return { content: [{ type: 'text', text: 'Not authenticated with Whoop. Use get_auth_url to authorize first.' }] };
        }
        client.setTokens(tokens);
        try {
          await sync.smartSync();
        } catch {
          // Continue with cached data
        }
      }

      switch (name) {
        case 'get_today': {
          const recovery = db.getLatestRecovery();
          const sleep = db.getLatestSleep();
          const cycle = db.getLatestCycle();

          if (!recovery && !sleep && !cycle) {
            return { content: [{ type: 'text', text: 'No data available. Try running sync_data first.' }] };
          }

          let response = "# Today's Whoop Summary\n\n";

          if (recovery) {
            response += `## Recovery: ${recovery.recovery_score ?? 'N/A'}% ${recovery.recovery_score ? getRecoveryZone(recovery.recovery_score) : ''}\n`;
            response += `- **HRV**: ${recovery.hrv_rmssd?.toFixed(1) ?? 'N/A'} ms\n`;
            response += `- **Resting HR**: ${recovery.resting_hr ?? 'N/A'} bpm\n`;
            if (recovery.spo2) response += `- **SpO2**: ${recovery.spo2.toFixed(1)}%\n`;
            if (recovery.skin_temp) response += `- **Skin Temp**: ${recovery.skin_temp.toFixed(1)}°C\n`;
            response += '\n';
          }

          if (sleep) {
            const totalSleep = (sleep.total_in_bed_milli ?? 0) - (sleep.total_awake_milli ?? 0);
            response += `## Last Night's Sleep\n`;
            response += `- **Total Sleep**: ${formatDuration(totalSleep)}\n`;
            response += `- **Performance**: ${sleep.sleep_performance?.toFixed(0) ?? 'N/A'}%\n`;
            response += `- **Efficiency**: ${sleep.sleep_efficiency?.toFixed(0) ?? 'N/A'}%\n`;
            response += `- **Stages**: Light ${formatDuration(sleep.total_light_milli)}, Deep ${formatDuration(sleep.total_deep_milli)}, REM ${formatDuration(sleep.total_rem_milli)}\n`;
            if (sleep.respiratory_rate) response += `- **Respiratory Rate**: ${sleep.respiratory_rate.toFixed(1)} breaths/min\n`;
            response += '\n';
          }

          if (cycle) {
            response += `## Current Strain\n`;
            response += `- **Day Strain**: ${cycle.strain?.toFixed(1) ?? 'N/A'} ${cycle.strain ? getStrainZone(cycle.strain) : ''}\n`;
            if (cycle.kilojoule) response += `- **Calories**: ${Math.round(cycle.kilojoule / 4.184)} kcal\n`;
            if (cycle.avg_hr) response += `- **Avg HR**: ${cycle.avg_hr} bpm\n`;
            if (cycle.max_hr) response += `- **Max HR**: ${cycle.max_hr} bpm\n`;
          }

          return { content: [{ type: 'text', text: response }] };
        }

        case 'get_recovery_trends': {
          const days = validateDays(typedArgs.days);
          const trends = db.getRecoveryTrends(days);

          if (trends.length === 0) {
            return { content: [{ type: 'text', text: 'No recovery data available for the requested period.' }] };
          }

          let response = `# Recovery Trends (Last ${days} Days)\n\n`;
          response += '| Date | Recovery | HRV | RHR |\n|------|----------|-----|-----|\n';

          for (const day of trends) {
            response += `| ${formatDate(day.date)} | ${day.recovery_score}% | ${day.hrv?.toFixed(1) ?? 'N/A'} ms | ${day.rhr ?? 'N/A'} bpm |\n`;
          }

          const avgRecovery = trends.reduce((sum, d) => sum + (d.recovery_score || 0), 0) / trends.length;
          const avgHrv = trends.reduce((sum, d) => sum + (d.hrv || 0), 0) / trends.length;
          const avgRhr = trends.reduce((sum, d) => sum + (d.rhr || 0), 0) / trends.length;

          response += `\n## Averages\n- **Recovery**: ${avgRecovery.toFixed(0)}%\n- **HRV**: ${avgHrv.toFixed(1)} ms\n- **RHR**: ${avgRhr.toFixed(0)} bpm\n`;

          return { content: [{ type: 'text', text: response }] };
        }

        case 'get_sleep_analysis': {
          const days = validateDays(typedArgs.days);
          const trends = db.getSleepTrends(days);

          if (trends.length === 0) {
            return { content: [{ type: 'text', text: 'No sleep data available for the requested period.' }] };
          }

          let response = `# Sleep Analysis (Last ${days} Days)\n\n`;
          response += '| Date | Duration | Performance | Efficiency |\n|------|----------|-------------|------------|\n';

          for (const day of trends) {
            response += `| ${formatDate(day.date)} | ${day.total_sleep_hours?.toFixed(1) ?? 'N/A'}h | ${day.performance?.toFixed(0) ?? 'N/A'}% | ${day.efficiency?.toFixed(0) ?? 'N/A'}% |\n`;
          }

          const avgDuration = trends.reduce((sum, d) => sum + (d.total_sleep_hours || 0), 0) / trends.length;
          const avgPerf = trends.reduce((sum, d) => sum + (d.performance || 0), 0) / trends.length;
          const avgEff = trends.reduce((sum, d) => sum + (d.efficiency || 0), 0) / trends.length;

          response += `\n## Averages\n- **Duration**: ${avgDuration.toFixed(1)} hours\n- **Performance**: ${avgPerf.toFixed(0)}%\n- **Efficiency**: ${avgEff.toFixed(0)}%\n`;

          return { content: [{ type: 'text', text: response }] };
        }

        case 'get_strain_history': {
          const days = validateDays(typedArgs.days);
          const trends = db.getStrainTrends(days);

          if (trends.length === 0) {
            return { content: [{ type: 'text', text: 'No strain data available for the requested period.' }] };
          }

          let response = `# Strain History (Last ${days} Days)\n\n`;
          response += '| Date | Strain | Calories |\n|------|--------|----------|\n';

          for (const day of trends) {
            response += `| ${formatDate(day.date)} | ${day.strain?.toFixed(1) ?? 'N/A'} | ${day.calories ?? 'N/A'} kcal |\n`;
          }

          const avgStrain = trends.reduce((sum, d) => sum + (d.strain || 0), 0) / trends.length;
          const avgCalories = trends.reduce((sum, d) => sum + (d.calories || 0), 0) / trends.length;

          response += `\n## Averages\n- **Daily Strain**: ${avgStrain.toFixed(1)}\n- **Daily Calories**: ${Math.round(avgCalories)} kcal\n`;

          return { content: [{ type: 'text', text: response }] };
        }

        case 'get_workouts': {
          const days = validateDays(typedArgs.days);
          const endDate = new Date();
          const startDate = new Date();
          startDate.setDate(startDate.getDate() - days);
          const workouts = db.getWorkoutsByDateRange(startDate.toISOString(), endDate.toISOString());

          if (workouts.length === 0) {
            return { content: [{ type: 'text', text: 'No workout data available for the requested period. If you logged a workout on your Whoop strap or app, try sync_data first.' }] };
          }

          let response = `# Workouts (Last ${days} Days)\n\n`;
          response += '| Date | Sport | Duration | Avg HR | Max HR | Strain | Calories |\n|------|-------|----------|--------|--------|--------|----------|\n';

          for (const w of workouts) {
            const durationMs = new Date(w.end_time).getTime() - new Date(w.start_time).getTime();
            const calories = w.kilojoule ? Math.round(w.kilojoule / 4.184) : null;
            response += `| ${formatDate(w.start_time)} | ${getSportName(w.sport_id)} | ${formatDuration(durationMs)} | ${w.avg_hr ?? 'N/A'} bpm | ${w.max_hr ?? 'N/A'} bpm | ${w.strain?.toFixed(1) ?? 'N/A'} | ${calories ?? 'N/A'} kcal |\n`;
          }

          response += '\nHinweis: Der Strain-Wert misst kardiovaskulaere Belastung (Puls ueber Zeit) und unterschaetzt strukturell die Belastung von Krafttraining, da Pausen zwischen Saetzen den Puls senken. Ein niedriger Strain-Wert bei Krafttraining ist normal und kein Zeichen von zu wenig Intensitaet.';

          return { content: [{ type: 'text', text: response }] };
        }

        case 'sync_data': {
          const tokens = db.getTokens();
          if (!tokens) {
            return { content: [{ type: 'text', text: 'Not authenticated with Whoop. Use get_auth_url to authorize first.' }] };
          }
          client.setTokens(tokens);

          const full = validateBoolean(typedArgs.full);
          let stats;

          if (full) {
            stats = await sync.syncDays(90);
          } else {
            const result = await sync.smartSync();
            if (result.type === 'skip') {
              return { content: [{ type: 'text', text: 'Data is already up to date (synced within the last hour).' }] };
            }
            stats = result.stats;
          }

          return {
            content: [{
              type: 'text',
              text: `Sync complete!\n- Cycles: ${stats?.cycles}\n- Recoveries: ${stats?.recoveries}\n- Sleeps: ${stats?.sleeps}\n- Workouts: ${stats?.workouts}`,
            }],
          };
        }

        case 'get_auth_url': {
          const scopes = ['read:profile', 'read:body_measurement', 'read:cycles', 'read:recovery', 'read:sleep', 'read:workout', 'offline'];
          const url = client.getAuthorizationUrl(scopes);
          return {
            content: [{
              type: 'text',
              text: `To authorize with Whoop:\n\n1. Visit: ${url}\n2. Log in and authorize\n3. You'll be redirected back automatically\n\nRedirect URI: ${config.redirectUri}`,
            }],
          };
        }

        default:
          throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${name}`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return { content: [{ type: 'text', text: `Error: ${message}` }], isError: true };
    }
  });

  return server;
}

// ============================================================================
// Multi-tenant customer accounts (email code login + per-customer Whoop connect)
// ============================================================================

const LOGIN_CODE_TTL_MS = 10 * 60 * 1000; // 10 minutes
const SESSION_TOKEN_TTL_MS = 180 * 24 * 60 * 60 * 1000; // 180 days
const WHOOP_SCOPES = ['read:profile', 'read:body_measurement', 'read:cycles', 'read:recovery', 'read:sleep', 'read:workout', 'offline'];

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function stateSecret(): string {
  return config.clientSecret || 'dev-fallback-secret';
}

function signState(customerId: string): string {
  const sig = createHmac('sha256', stateSecret()).update(customerId).digest('hex').slice(0, 16);
  return `${customerId}.${sig}`;
}

function verifyState(state: string): string | null {
  const idx = state.lastIndexOf('.');
  if (idx === -1) return null;
  const customerId = state.slice(0, idx);
  const sig = state.slice(idx + 1);
  const expected = createHmac('sha256', stateSecret()).update(customerId).digest('hex').slice(0, 16);
  return sig === expected ? customerId : null;
}

// One WhoopClient per customer, cached across requests so token refreshes
// don't get lost between calls. Tokens are (re)loaded from the DB on demand.
const customerClients = new Map<string, WhoopClient>();

function getCustomerClient(customerId: string): WhoopClient {
  let c = customerClients.get(customerId);
  if (!c) {
    c = new WhoopClient({
      clientId: config.clientId,
      clientSecret: config.clientSecret,
      redirectUri: config.redirectUri,
      onTokenRefresh: tokens => db.saveCustomerWhoopTokens(customerId, tokens),
    });
    customerClients.set(customerId, c);
  }
  return c;
}

interface AuthedRequest extends Request {
  customerId?: string;
}

/** Strict session auth — used for the customer-only endpoints (connect-url, status). */
function requireSession(req: AuthedRequest, res: Response, next: NextFunction): void {
  const authHeader = req.headers.authorization;
  const token = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
  const customerId = token ? db.getCustomerIdForSession(token) : null;
  if (!customerId) {
    res.status(401).json({ error: 'unauthorized' });
    return;
  }
  req.customerId = customerId;
  next();
}

/**
 * Dual-mode auth for the data endpoints (dashboard/checklist/journal):
 * a valid session maps to that customer's own data; the legacy shared
 * APP_API_KEY (used by the original single-owner app build) keeps working
 * and maps to the legacy owner scope (customerId = null), so nothing already
 * deployed breaks before it's switched over to logging in.
 */
function resolveScopeOrNull(req: Request): { ok: true; customerId: string | null } | { ok: false } {
  const authHeader = req.headers.authorization;
  const token = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
  if (token) {
    const customerId = db.getCustomerIdForSession(token);
    if (customerId) return { ok: true, customerId };
  }
  const APP_API_KEY = process.env.APP_API_KEY ?? '';
  if (APP_API_KEY && req.query.key === APP_API_KEY) {
    return { ok: true, customerId: null };
  }
  return { ok: false };
}

async function main(): Promise<void> {
  if (config.mode === 'stdio') {
    const server = createMcpServer();
    const transport = new StdioServerTransport();
    await server.connect(transport);
    process.stderr.write('Whoop MCP server running on stdio\n');
  } else {
    const app = express();
    app.use(express.json());

    app.get('/callback', async (req: Request, res: Response) => {
      const code = req.query.code as string | undefined;
      const state = req.query.state as string | undefined;
      if (!code) {
        res.status(400).send('Missing authorization code');
        return;
      }

      const customerId = state ? verifyState(state) : null;

      if (customerId) {
        try {
          const customerClient = getCustomerClient(customerId);
          const tokens = await customerClient.exchangeCodeForTokens(code);
          db.saveCustomerWhoopTokens(customerId, tokens);
          customerClient.setTokens(tokens);
          const customerSync = new WhoopSync(customerClient, db, customerId);
          customerSync.syncDays(90).catch(() => {});
          res.send(
            '<html><body style="font-family:-apple-system,sans-serif;text-align:center;padding:60px 20px;">' +
              '<h2>Verbunden!</h2><p>Du kannst dieses Fenster jetzt schließen und zur App zurückkehren.</p>' +
              '</body></html>'
          );
        } catch (error) {
          console.error('Customer OAuth callback error:', error);
          res.status(500).send('Authorization failed: ' + (error instanceof Error ? error.message : String(error)));
        }
        return;
      }

      try {
        const tokens = await client.exchangeCodeForTokens(code);
        db.saveTokens(tokens);
        sync.syncDays(90).catch(() => {});
        res.send('Authorization successful! You can close this window.');
      } catch (error) {
        console.error('OAuth callback error:', error);
        res.status(500).send('Authorization failed: ' + (error instanceof Error ? error.message : String(error)));
      }
    });

    app.get('/health', (_req: Request, res: Response) => {
      res.json({ status: 'ok', authenticated: Boolean(db.getTokens()) });
    });

    app.all('/mcp', async (req: Request, res: Response) => {
      const sessionId = req.headers['mcp-session-id'] as string | undefined;

      if (req.method === 'DELETE' && sessionId && transports.has(sessionId)) {
        const session = transports.get(sessionId)!;
        await session.transport.close();
        transports.delete(sessionId);
        res.status(200).send('Session closed');
        return;
      }

      if (req.method === 'POST') {
        let transport: StreamableHTTPServerTransport;

        if (sessionId && transports.has(sessionId)) {
          const session = transports.get(sessionId)!;
          session.lastAccess = Date.now();
          transport = session.transport;
        } else {
          transport = new StreamableHTTPServerTransport({
            sessionIdGenerator: () => crypto.randomUUID(),
            onsessioninitialized: newSessionId => {
              transports.set(newSessionId, { transport, lastAccess: Date.now() });
            },
          });

          const server = createMcpServer();
          await server.connect(transport);
        }

        await transport.handleRequest(req, res, req.body);
        return;
      }

      res.status(405).send('Method not allowed');
    });

    app.get('/sse', (_req: Request, res: Response) => {
      res.status(410).send('SSE endpoint deprecated. Use /mcp with Streamable HTTP transport.');
    });

    // ---------- Customer auth: request a login code, verify it ----------

    app.options('/api/auth/request-code', (_req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
      res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
      res.sendStatus(204);
    });

    app.post('/api/auth/request-code', async (req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const email = String((req.body ?? {}).email ?? '').trim().toLowerCase();
      if (!EMAIL_RE.test(email)) {
        res.status(400).json({ error: 'invalid_email' });
        return;
      }

      const code = randomInt(0, 1_000_000).toString().padStart(6, '0');
      db.saveLoginCode(email, code, LOGIN_CODE_TTL_MS);

      try {
        await sendLoginCodeEmail(email, code);
      } catch (error) {
        console.error('Failed to send login code email:', error);
        res.status(502).json({ error: 'email_send_failed' });
        return;
      }

      res.json({ ok: true });
    });

    app.options('/api/auth/verify-code', (_req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
      res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
      res.sendStatus(204);
    });

    app.post('/api/auth/verify-code', (req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const email = String((req.body ?? {}).email ?? '').trim().toLowerCase();
      const code = String((req.body ?? {}).code ?? '').trim();
      if (!EMAIL_RE.test(email) || !code) {
        res.status(400).json({ error: 'invalid_request' });
        return;
      }

      const result = db.verifyLoginCode(email, code);
      if (result === 'expired') {
        res.status(400).json({ error: 'expired' });
        return;
      }
      if (result === 'invalid') {
        res.status(401).json({ error: 'invalid_code' });
        return;
      }

      const customer = db.findOrCreateCustomer(email);
      const sessionToken = db.createSession(customer.id, SESSION_TOKEN_TTL_MS);
      res.json({ sessionToken, customerId: customer.id, email: customer.email });
    });

    // ---------- Customer Whoop connection ----------

    app.get('/api/whoop/connect-url', requireSession as express.RequestHandler, (req: AuthedRequest, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const customerId = req.customerId!;
      const url = getCustomerClient(customerId).getAuthorizationUrl(WHOOP_SCOPES, signState(customerId));
      res.json({ url });
    });

    app.get('/api/whoop/status', requireSession as express.RequestHandler, (req: AuthedRequest, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const customerId = req.customerId!;
      res.json({ connected: db.isCustomerWhoopConnected(customerId) });
    });

    // ---------- App data endpoints (session auth, with legacy APP_API_KEY fallback) ----------

    app.get('/api/dashboard', async (req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const scope = resolveScopeOrNull(req);
      if (!scope.ok) {
        res.status(401).json({ error: 'unauthorized' });
        return;
      }
      const { customerId } = scope;

      let tokens;
      let dashClient: WhoopClient;
      let dashSync: WhoopSync;

      if (customerId === null) {
        tokens = db.getTokens();
        dashClient = client;
        dashSync = sync;
      } else {
        tokens = db.getCustomerWhoopTokens(customerId);
        dashClient = getCustomerClient(customerId);
        dashSync = new WhoopSync(dashClient, db, customerId);
      }

      if (!tokens) {
        res.status(200).json({ authenticated: false });
        return;
      }
      dashClient.setTokens(tokens);
      try {
        await dashSync.smartSync();
      } catch {
        // continue with cached data
      }

      const recovery = db.getLatestRecovery(customerId);
      const sleep = db.getLatestSleep(customerId);
      const cycle = db.getLatestCycle(customerId);
      const trendDays = validateDays(req.query.days);
      const recoveryTrends = db.getRecoveryTrends(trendDays, customerId);
      const sleepTrends = db.getSleepTrends(trendDays, customerId);
      const strainTrends = db.getStrainTrends(trendDays, customerId);

      res.json({
        authenticated: true,
        recovery: recovery
          ? {
              score: recovery.recovery_score,
              hrv: recovery.hrv_rmssd,
              restingHr: recovery.resting_hr,
              spo2: recovery.spo2,
              skinTemp: recovery.skin_temp,
            }
          : null,
        sleep: sleep
          ? {
              totalSleepMilli: (sleep.total_in_bed_milli ?? 0) - (sleep.total_awake_milli ?? 0),
              performance: sleep.sleep_performance,
              efficiency: sleep.sleep_efficiency,
              lightMilli: sleep.total_light_milli,
              deepMilli: sleep.total_deep_milli,
              remMilli: sleep.total_rem_milli,
              respiratoryRate: sleep.respiratory_rate,
            }
          : null,
        cycle: cycle
          ? {
              strain: cycle.strain,
              kilojoule: cycle.kilojoule,
              avgHr: cycle.avg_hr,
              maxHr: cycle.max_hr,
            }
          : null,
        recoveryTrends,
        sleepTrends,
        strainTrends,
      });
    });

    app.options('/api/checklist', (_req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
      res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
      res.sendStatus(204);
    });

    app.get('/api/checklist', (req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const scope = resolveScopeOrNull(req);
      if (!scope.ok) {
        res.status(401).json({ error: 'unauthorized' });
        return;
      }
      const date = (req.query.date as string) ?? new Date().toISOString().slice(0, 10);
      const state = db.getChecklist(date, scope.customerId);
      res.json({ date, state });
    });

    app.post('/api/checklist', (req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const scope = resolveScopeOrNull(req);
      if (!scope.ok) {
        res.status(401).json({ error: 'unauthorized' });
        return;
      }
      const { date, taskId, done } = req.body ?? {};
      if (!date || !taskId || typeof done !== 'boolean') {
        res.status(400).json({ error: 'date, taskId, done required' });
        return;
      }
      db.setChecklistItem(date, taskId, done, scope.customerId);
      res.json({ ok: true });
    });

    app.get('/api/journal', (req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const scope = resolveScopeOrNull(req);
      if (!scope.ok) {
        res.status(401).json({ error: 'unauthorized' });
        return;
      }
      const date = (req.query.date as string) ?? new Date().toISOString().slice(0, 10);
      const entry = db.getJournal(date, scope.customerId);
      res.json({ date, entry });
    });

    app.post('/api/journal', (req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const scope = resolveScopeOrNull(req);
      if (!scope.ok) {
        res.status(401).json({ error: 'unauthorized' });
        return;
      }
      const {
        date,
        alcohol,
        alcoholLastTime,
        caffeineCount,
        caffeineLastTime,
        sauna,
        coldExposure,
        lateMeal,
        screenTime,
        meditation,
        stretching,
        nap,
        sick,
        travel,
        mood,
        stress,
        notes,
      } = req.body ?? {};
      if (!date) {
        res.status(400).json({ error: 'date required' });
        return;
      }
      db.saveJournal(
        date,
        {
          alcohol,
          alcoholLastTime,
          caffeineCount,
          caffeineLastTime,
          sauna,
          coldExposure,
          lateMeal,
          screenTime,
          meditation,
          stretching,
          nap,
          sick,
          travel,
          mood,
          stress,
          notes,
        },
        scope.customerId
      );
      res.json({ ok: true });
    });

    app.get('/api/journal-settings', (req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const scope = resolveScopeOrNull(req);
      if (!scope.ok) {
        res.status(401).json({ error: 'unauthorized' });
        return;
      }
      const enabledFields = db.getJournalSettings(scope.customerId);
      res.json({ enabledFields });
    });

    app.post('/api/journal-settings', (req: Request, res: Response) => {
      res.header('Access-Control-Allow-Origin', '*');
      const scope = resolveScopeOrNull(req);
      if (!scope.ok) {
        res.status(401).json({ error: 'unauthorized' });
        return;
      }
      const { enabledFields } = req.body ?? {};
      if (!Array.isArray(enabledFields)) {
        res.status(400).json({ error: 'enabledFields array required' });
        return;
      }
      db.saveJournalSettings(enabledFields, scope.customerId);
      res.json({ ok: true });
    });
    app.get('/api/day', async (req: Request, res: Response) => {
            res.header('Access-Control-Allow-Origin', '*');
            const scope = resolveScopeOrNull(req);
            if (!scope.ok) {
                      res.status(401).json({ error: 'unauthorized' });
                      return;
            }
            const date = (req.query.date as string) ?? new Date().toISOString().slice(0, 10);
            const startDate = `${date}T00:00:00.000Z`;
            const endDate = `${date}T23:59:59.999Z`;
            const recoveries = db.getRecoveriesByDateRange(startDate, endDate, scope.customerId);
            const sleeps = db.getSleepsByDateRange(startDate, endDate, false, scope.customerId);
            const cycles = db.getCyclesByDateRange(startDate, endDate, scope.customerId);
            const recovery = recoveries[0] ?? null;
            const sleep = sleeps[0] ?? null;
            const cycle = cycles[0] ?? null;
            const entry = db.getJournal(date, scope.customerId);
            res.json({
                      date,
                      recovery: recovery ? { score: recovery.recovery_score, hrv: recovery.hrv_rmssd, restingHr: recovery.resting_hr } : null,
                      sleep: sleep ? { totalSleepMilli: (sleep.total_in_bed_milli ?? 0) - (sleep.total_awake_milli ?? 0), performance: sleep.sleep_performance, efficiency: sleep.sleep_efficiency } : null,
                      cycle: cycle ? { strain: cycle.strain, kilojoule: cycle.kilojoule, avgHr: cycle.avg_hr, maxHr: cycle.max_hr } : null,
                      journal: entry,
            });
    });
    
    const server = app.listen(config.port, '0.0.0.0', () => {
      process.stdout.write(`Whoop MCP server running on http://0.0.0.0:${config.port}\n`);
    });

    const shutdown = (): void => {
      process.stdout.write('\nShutting down...\n');
      for (const [, session] of transports) {
        session.transport.close().catch(() => {});
      }
      transports.clear();
      db.close();
      server.close(() => process.exit(0));
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
  }
}

main().catch(error => {
  process.stderr.write(`Fatal error: ${error}\n`);
  process.exit(1);
});
