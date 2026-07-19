import Database from 'better-sqlite3';
import { randomUUID, randomBytes } from 'node:crypto';
import { encrypt, decrypt, isEncrypted } from './crypto.js';
import type {
  WhoopTokens,
  WhoopCycle,
  WhoopRecovery,
  WhoopSleep,
  WhoopWorkout,
  DbCycle,
  DbRecovery,
  DbSleep,
  DbWorkout,
} from './types.js';

interface TokenRow {
  id: number;
  access_token: string;
  refresh_token: string;
  expires_at: number;
  updated_at: string;
}

interface CustomerWhoopTokenRow {
  customer_id: string;
  access_token: string;
  refresh_token: string;
  expires_at: number;
  updated_at: string;
}

interface SyncStateRow {
  id: number;
  last_sync_at: string | null;
  oldest_synced_date: string | null;
  newest_synced_date: string | null;
}

interface CustomerSyncStateRow {
  customer_id: string;
  last_sync_at: string | null;
  oldest_synced_date: string | null;
  newest_synced_date: string | null;
}

interface RecoveryTrendRow {
  date: string;
  recovery_score: number;
  hrv: number;
  rhr: number;
}

interface SleepTrendRow {
  date: string;
  total_sleep_hours: number;
  performance: number;
  efficiency: number;
}

interface StrainTrendRow {
  date: string;
  strain: number;
  calories: number;
}

export interface Customer {
  id: string;
  email: string;
  created_at: string;
}

export class WhoopDatabase {
  private db: Database.Database;

  constructor(dbPath = './whoop.db') {
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.initSchema();
    this.migrateJournalColumns();
    this.migrateCustomerColumns();
    this.migrateToKeyedTables();
  }

  private initSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS tokens (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        access_token TEXT NOT NULL,
        refresh_token TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS sync_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        last_sync_at TEXT,
        oldest_synced_date TEXT,
        newest_synced_date TEXT
      );

      CREATE TABLE IF NOT EXISTS cycles (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT,
        score_state TEXT NOT NULL,
        strain REAL,
        kilojoule REAL,
        avg_hr INTEGER,
        max_hr INTEGER,
        synced_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS recovery (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        sleep_id TEXT,
        created_at TEXT NOT NULL,
        score_state TEXT NOT NULL,
        recovery_score INTEGER,
        resting_hr INTEGER,
        hrv_rmssd REAL,
        spo2 REAL,
        skin_temp REAL,
        synced_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS sleep (
        id TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        cycle_id INTEGER,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        is_nap INTEGER NOT NULL DEFAULT 0,
        score_state TEXT NOT NULL,
        total_in_bed_milli INTEGER,
        total_awake_milli INTEGER,
        total_light_milli INTEGER,
        total_deep_milli INTEGER,
        total_rem_milli INTEGER,
        sleep_performance REAL,
        sleep_efficiency REAL,
        sleep_consistency REAL,
        respiratory_rate REAL,
        sleep_needed_baseline_milli INTEGER,
        sleep_needed_debt_milli INTEGER,
        sleep_needed_strain_milli INTEGER,
        synced_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS workouts (
        id TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        sport_id INTEGER NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        score_state TEXT NOT NULL,
        strain REAL,
        avg_hr INTEGER,
        max_hr INTEGER,
        kilojoule REAL,
        zone_zero_milli INTEGER,
        zone_one_milli INTEGER,
        zone_two_milli INTEGER,
        zone_three_milli INTEGER,
        zone_four_milli INTEGER,
        zone_five_milli INTEGER,
        synced_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_cycles_start ON cycles(start_time);
      CREATE INDEX IF NOT EXISTS idx_recovery_created ON recovery(created_at);
      CREATE INDEX IF NOT EXISTS idx_sleep_start ON sleep(start_time);
      CREATE INDEX IF NOT EXISTS idx_workouts_start ON workouts(start_time);

      -- Legacy single-owner tables. Their primary keys (date) / (date, task_id)
      -- cannot safely hold more than one person's rows per date, so they are kept
      -- only as a migration source for the correctly-keyed *_v2 tables below and
      -- are no longer written to.
      CREATE TABLE IF NOT EXISTS checklist (
        date TEXT NOT NULL,
        task_id TEXT NOT NULL,
        done INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (date, task_id)
      );

      CREATE TABLE IF NOT EXISTS journal (
        date TEXT PRIMARY KEY,
        alcohol INTEGER,
        alcohol_last_time TEXT,
        caffeine_count INTEGER,
        caffeine_last_time TEXT,
        sauna INTEGER,
        cold_exposure INTEGER,
        late_meal INTEGER,
        screen_time INTEGER,
        meditation INTEGER,
        stretching INTEGER,
        nap INTEGER,
        sick INTEGER,
        travel INTEGER,
        mood INTEGER,
        stress INTEGER,
        notes TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS journal_settings (
        id TEXT PRIMARY KEY,
        enabled_fields TEXT
      );

      -- customer_key = 'owner' for the legacy single-owner app, or a customer's
      -- id for everyone else. Properly composite-keyed so multiple customers
      -- (or the owner + customers) can share the same calendar date.
      CREATE TABLE IF NOT EXISTS checklist_v2 (
        customer_key TEXT NOT NULL DEFAULT 'owner',
        date TEXT NOT NULL,
        task_id TEXT NOT NULL,
        done INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (customer_key, date, task_id)
      );

      CREATE TABLE IF NOT EXISTS journal_v2 (
        customer_key TEXT NOT NULL DEFAULT 'owner',
        date TEXT NOT NULL,
        alcohol INTEGER,
        alcohol_last_time TEXT,
        caffeine_count INTEGER,
        caffeine_last_time TEXT,
        sauna INTEGER,
        cold_exposure INTEGER,
        late_meal INTEGER,
        screen_time INTEGER,
        meditation INTEGER,
        stretching INTEGER,
        nap INTEGER,
        sick INTEGER,
        travel INTEGER,
        mood INTEGER,
        stress INTEGER,
        notes TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (customer_key, date)
      );

      INSERT OR IGNORE INTO sync_state (id) VALUES (1);

      CREATE TABLE IF NOT EXISTS customers (
        id TEXT PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS login_codes (
        email TEXT PRIMARY KEY,
        code TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        expires_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_sessions_customer ON sessions(customer_id);

      CREATE TABLE IF NOT EXISTS customer_whoop_tokens (
        customer_id TEXT PRIMARY KEY,
        access_token TEXT NOT NULL,
        refresh_token TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS customer_sync_state (
        customer_id TEXT PRIMARY KEY,
        last_sync_at TEXT,
        oldest_synced_date TEXT,
        newest_synced_date TEXT
      );
    `);
  }

  private migrateJournalColumns(): void {
    const columns: [string, string][] = [
      ['alcohol_last_time', 'TEXT'],
      ['caffeine_last_time', 'TEXT'],
      ['sauna', 'INTEGER'],
      ['cold_exposure', 'INTEGER'],
      ['late_meal', 'INTEGER'],
      ['screen_time', 'INTEGER'],
      ['meditation', 'INTEGER'],
      ['stretching', 'INTEGER'],
      ['nap', 'INTEGER'],
      ['sick', 'INTEGER'],
      ['travel', 'INTEGER'],
    ];
    for (const [name, type] of columns) {
      try {
        this.db.exec(`ALTER TABLE journal ADD COLUMN ${name} ${type}`);
      } catch (e) {
        // column already exists, ignore
      }
    }
    this.db.exec(`
      INSERT OR IGNORE INTO journal_settings (id, enabled_fields)
      VALUES ('default', '["alcohol","caffeine","sauna","cold_exposure","late_meal","screen_time","meditation","stretching","nap","sick","travel"]')
    `);
  }

  // Adds customer_id scoping to tables that originally only supported a single
  // owner (you, via the legacy `tokens` singleton + MCP tool handlers). Existing
  // rows keep customer_id = NULL, which the "owner" queries below treat as their
  // own scope, so nothing about the existing Claude Desktop / MCP integration
  // changes. New rows written on behalf of app customers get a real customer_id.
  private migrateCustomerColumns(): void {
    const tables = ['cycles', 'recovery', 'sleep', 'workouts', 'checklist', 'journal', 'journal_settings'];
    for (const table of tables) {
      try {
        this.db.exec(`ALTER TABLE ${table} ADD COLUMN customer_id TEXT`);
      } catch (e) {
        // column already exists, ignore
      }
    }
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_cycles_customer ON cycles(customer_id, start_time)`);
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_recovery_customer ON recovery(customer_id, created_at)`);
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_sleep_customer ON sleep(customer_id, start_time)`);
    this.db.exec(`CREATE INDEX IF NOT EXISTS idx_workouts_customer ON workouts(customer_id, start_time)`);
  }

  // One-time (idempotent, safe to run every startup) copy of the legacy
  // owner-only checklist/journal rows into the properly-keyed *_v2 tables.
  // Only rows that predate the customer_id column (i.e. customer_id IS NULL)
  // exist in the old tables, so this only ever imports the owner's own data.
  private migrateToKeyedTables(): void {
    this.db.exec(`
      INSERT OR IGNORE INTO checklist_v2 (customer_key, date, task_id, done, updated_at)
      SELECT 'owner', date, task_id, done, updated_at FROM checklist WHERE customer_id IS NULL
    `);
    this.db.exec(`
      INSERT OR IGNORE INTO journal_v2 (
        customer_key, date, alcohol, alcohol_last_time, caffeine_count, caffeine_last_time,
        sauna, cold_exposure, late_meal, screen_time, meditation, stretching, nap, sick, travel,
        mood, stress, notes, updated_at
      )
      SELECT 'owner', date, alcohol, alcohol_last_time, caffeine_count, caffeine_last_time,
        sauna, cold_exposure, late_meal, screen_time, meditation, stretching, nap, sick, travel,
        mood, stress, notes, updated_at
      FROM journal WHERE customer_id IS NULL
    `);
  }

  // ---------- Legacy single-owner tokens (Claude Desktop / MCP tools) ----------

  saveTokens(tokens: WhoopTokens): void {
    const encryptedAccess = encrypt(tokens.access_token);
    const encryptedRefresh = encrypt(tokens.refresh_token);

    this.db.prepare(`
      INSERT OR REPLACE INTO tokens (id, access_token, refresh_token, expires_at, updated_at)
      VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
    `).run(encryptedAccess, encryptedRefresh, tokens.expires_at);
  }

  getTokens(): WhoopTokens | null {
    const row = this.db.prepare('SELECT * FROM tokens WHERE id = 1').get() as TokenRow | undefined;
    if (!row) return null;

    const accessToken = isEncrypted(row.access_token) ? decrypt(row.access_token) : row.access_token;
    const refreshToken = isEncrypted(row.refresh_token) ? decrypt(row.refresh_token) : row.refresh_token;

    return {
      access_token: accessToken,
      refresh_token: refreshToken,
      expires_at: row.expires_at,
    };
  }

  getSyncState(): { lastSyncAt: string | null; oldestDate: string | null; newestDate: string | null } {
    const row = this.db.prepare('SELECT * FROM sync_state WHERE id = 1').get() as SyncStateRow | undefined;
    return {
      lastSyncAt: row?.last_sync_at ?? null,
      oldestDate: row?.oldest_synced_date ?? null,
      newestDate: row?.newest_synced_date ?? null,
    };
  }

  updateSyncState(oldestDate: string, newestDate: string): void {
    this.db.prepare(`
      UPDATE sync_state
      SET last_sync_at = CURRENT_TIMESTAMP,
          oldest_synced_date = COALESCE(
            CASE WHEN oldest_synced_date IS NULL OR ? < oldest_synced_date THEN ? ELSE oldest_synced_date END,
            ?
          ),
          newest_synced_date = COALESCE(
            CASE WHEN newest_synced_date IS NULL OR ? > newest_synced_date THEN ? ELSE newest_synced_date END,
            ?
          )
      WHERE id = 1
    `).run(oldestDate, oldestDate, oldestDate, newestDate, newestDate, newestDate);
  }

  // ---------- Customer accounts (email code login) ----------

  findCustomerByEmail(email: string): Customer | null {
    const row = this.db.prepare('SELECT * FROM customers WHERE email = ?').get(email.toLowerCase()) as Customer | undefined;
    return row ?? null;
  }

  findOrCreateCustomer(email: string): Customer {
    const normalized = email.toLowerCase().trim();
    const existing = this.findCustomerByEmail(normalized);
    if (existing) return existing;

    const id = randomUUID();
    this.db.prepare('INSERT INTO customers (id, email) VALUES (?, ?)').run(id, normalized);
    return this.findCustomerByEmail(normalized)!;
  }

  saveLoginCode(email: string, code: string, ttlMs: number): void {
    const normalized = email.toLowerCase().trim();
    const expiresAt = Date.now() + ttlMs;
    this.db.prepare(`
      INSERT INTO login_codes (email, code, expires_at, attempts)
      VALUES (?, ?, ?, 0)
      ON CONFLICT(email) DO UPDATE SET code = excluded.code, expires_at = excluded.expires_at, attempts = 0, created_at = CURRENT_TIMESTAMP
    `).run(normalized, code, expiresAt);
  }

  /**
   * Verifies a login code. Returns 'ok', 'invalid' (wrong code / no code requested),
   * or 'expired'. Tracks attempts and invalidates the code after 5 wrong tries.
   */
  verifyLoginCode(email: string, code: string): 'ok' | 'invalid' | 'expired' {
    const normalized = email.toLowerCase().trim();
    const row = this.db.prepare('SELECT * FROM login_codes WHERE email = ?').get(normalized) as
      | { email: string; code: string; expires_at: number; attempts: number }
      | undefined;

    if (!row) return 'invalid';
    if (row.attempts >= 5) return 'invalid';
    if (Date.now() > row.expires_at) return 'expired';

    if (row.code !== code) {
      this.db.prepare('UPDATE login_codes SET attempts = attempts + 1 WHERE email = ?').run(normalized);
      return 'invalid';
    }

    this.db.prepare('DELETE FROM login_codes WHERE email = ?').run(normalized);
    return 'ok';
  }

  createSession(customerId: string, ttlMs: number): string {
    const token = randomBytes(32).toString('hex');
    const expiresAt = Date.now() + ttlMs;
    this.db.prepare('INSERT INTO sessions (token, customer_id, expires_at) VALUES (?, ?, ?)').run(token, customerId, expiresAt);
    return token;
  }

  getCustomerIdForSession(token: string): string | null {
    const row = this.db.prepare('SELECT customer_id, expires_at FROM sessions WHERE token = ?').get(token) as
      | { customer_id: string; expires_at: number }
      | undefined;
    if (!row) return null;
    if (Date.now() > row.expires_at) {
      this.db.prepare('DELETE FROM sessions WHERE token = ?').run(token);
      return null;
    }
    return row.customer_id;
  }

  deleteSession(token: string): void {
    this.db.prepare('DELETE FROM sessions WHERE token = ?').run(token);
  }

  // ---------- Per-customer Whoop tokens ----------

  saveCustomerWhoopTokens(customerId: string, tokens: WhoopTokens): void {
    const encryptedAccess = encrypt(tokens.access_token);
    const encryptedRefresh = encrypt(tokens.refresh_token);
    this.db.prepare(`
      INSERT INTO customer_whoop_tokens (customer_id, access_token, refresh_token, expires_at, updated_at)
      VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(customer_id) DO UPDATE SET
        access_token = excluded.access_token,
        refresh_token = excluded.refresh_token,
        expires_at = excluded.expires_at,
        updated_at = CURRENT_TIMESTAMP
    `).run(customerId, encryptedAccess, encryptedRefresh, tokens.expires_at);
  }

  getCustomerWhoopTokens(customerId: string): WhoopTokens | null {
    const row = this.db.prepare('SELECT * FROM customer_whoop_tokens WHERE customer_id = ?').get(customerId) as
      | CustomerWhoopTokenRow
      | undefined;
    if (!row) return null;
    const accessToken = isEncrypted(row.access_token) ? decrypt(row.access_token) : row.access_token;
    const refreshToken = isEncrypted(row.refresh_token) ? decrypt(row.refresh_token) : row.refresh_token;
    return {
      access_token: accessToken,
      refresh_token: refreshToken,
      expires_at: row.expires_at,
    };
  }

  isCustomerWhoopConnected(customerId: string): boolean {
    return this.getCustomerWhoopTokens(customerId) !== null;
  }

  getCustomerSyncState(customerId: string): { lastSyncAt: string | null; oldestDate: string | null; newestDate: string | null } {
    const row = this.db.prepare('SELECT * FROM customer_sync_state WHERE customer_id = ?').get(customerId) as
      | CustomerSyncStateRow
      | undefined;
    return {
      lastSyncAt: row?.last_sync_at ?? null,
      oldestDate: row?.oldest_synced_date ?? null,
      newestDate: row?.newest_synced_date ?? null,
    };
  }

  updateCustomerSyncState(customerId: string, oldestDate: string, newestDate: string): void {
    this.db.prepare(`
      INSERT INTO customer_sync_state (customer_id, last_sync_at, oldest_synced_date, newest_synced_date)
      VALUES (?, CURRENT_TIMESTAMP, ?, ?)
      ON CONFLICT(customer_id) DO UPDATE SET
        last_sync_at = CURRENT_TIMESTAMP,
        oldest_synced_date = CASE WHEN customer_sync_state.oldest_synced_date IS NULL OR ? < customer_sync_state.oldest_synced_date THEN ? ELSE customer_sync_state.oldest_synced_date END,
        newest_synced_date = CASE WHEN customer_sync_state.newest_synced_date IS NULL OR ? > customer_sync_state.newest_synced_date THEN ? ELSE customer_sync_state.newest_synced_date END
    `).run(customerId, oldestDate, newestDate, oldestDate, oldestDate, newestDate, newestDate);
  }

  // ---------- Whoop data upserts (customerId = null keeps legacy owner behavior) ----------

  upsertCycles(cycles: WhoopCycle[], customerId: string | null = null): void {
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO cycles (id, user_id, start_time, end_time, score_state, strain, kilojoule, avg_hr, max_hr, synced_at, customer_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
    `);

    const insertMany = this.db.transaction((items: WhoopCycle[]) => {
      for (const c of items) {
        stmt.run(
          c.id,
          c.user_id,
          c.start,
          c.end,
          c.score_state,
          c.score?.strain ?? null,
          c.score?.kilojoule ?? null,
          c.score?.average_heart_rate ?? null,
          c.score?.max_heart_rate ?? null,
          customerId
        );
      }
    });

    insertMany(cycles);
  }

  upsertRecoveries(recoveries: WhoopRecovery[], customerId: string | null = null): void {
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO recovery (id, user_id, sleep_id, created_at, score_state, recovery_score, resting_hr, hrv_rmssd, spo2, skin_temp, synced_at, customer_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
    `);

    const insertMany = this.db.transaction((items: WhoopRecovery[]) => {
      for (const r of items) {
        stmt.run(
          r.cycle_id,
          r.user_id,
          r.sleep_id,
          r.created_at,
          r.score_state,
          r.score?.recovery_score ?? null,
          r.score?.resting_heart_rate ?? null,
          r.score?.hrv_rmssd_milli ?? null,
          r.score?.spo2_percentage ?? null,
          r.score?.skin_temp_celsius ?? null,
          customerId
        );
      }
    });

    insertMany(recoveries);
  }

  upsertSleeps(sleeps: WhoopSleep[], customerId: string | null = null): void {
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO sleep (
        id, user_id, start_time, end_time, is_nap, score_state,
        total_in_bed_milli, total_awake_milli, total_light_milli, total_deep_milli, total_rem_milli,
        sleep_performance, sleep_efficiency, sleep_consistency, respiratory_rate,
        sleep_needed_baseline_milli, sleep_needed_debt_milli, sleep_needed_strain_milli, synced_at, customer_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
    `);

    const insertMany = this.db.transaction((items: WhoopSleep[]) => {
      for (const s of items) {
        stmt.run(
          s.id,
          s.user_id,
          s.start,
          s.end,
          s.nap ? 1 : 0,
          s.score_state,
          s.score?.stage_summary.total_in_bed_time_milli ?? null,
          s.score?.stage_summary.total_awake_time_milli ?? null,
          s.score?.stage_summary.total_light_sleep_time_milli ?? null,
          s.score?.stage_summary.total_slow_wave_sleep_time_milli ?? null,
          s.score?.stage_summary.total_rem_sleep_time_milli ?? null,
          s.score?.sleep_performance_percentage ?? null,
          s.score?.sleep_efficiency_percentage ?? null,
          s.score?.sleep_consistency_percentage ?? null,
          s.score?.respiratory_rate ?? null,
          s.score?.sleep_needed.baseline_milli ?? null,
          s.score?.sleep_needed.need_from_sleep_debt_milli ?? null,
          s.score?.sleep_needed.need_from_recent_strain_milli ?? null,
          customerId
        );
      }
    });

    insertMany(sleeps);
  }

  upsertWorkouts(workouts: WhoopWorkout[], customerId: string | null = null): void {
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO workouts (
        id, user_id, sport_id, start_time, end_time, score_state,
        strain, avg_hr, max_hr, kilojoule,
        zone_zero_milli, zone_one_milli, zone_two_milli, zone_three_milli, zone_four_milli, zone_five_milli,
        synced_at, customer_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
    `);

    const insertMany = this.db.transaction((items: WhoopWorkout[]) => {
      for (const w of items) {
        stmt.run(
          w.id,
          w.user_id,
          w.sport_id,
          w.start,
          w.end,
          w.score_state,
          w.score?.strain ?? null,
          w.score?.average_heart_rate ?? null,
          w.score?.max_heart_rate ?? null,
          w.score?.kilojoule ?? null,
          w.score?.zone_duration?.zone_zero_milli ?? null,
          w.score?.zone_duration?.zone_one_milli ?? null,
          w.score?.zone_duration?.zone_two_milli ?? null,
          w.score?.zone_duration?.zone_three_milli ?? null,
          w.score?.zone_duration?.zone_four_milli ?? null,
          w.score?.zone_duration?.zone_five_milli ?? null,
          customerId
        );
      }
    });

    insertMany(workouts);
  }

  // ---------- Whoop data reads (customerId = null keeps legacy owner behavior) ----------

  getLatestCycle(customerId: string | null = null): DbCycle | null {
    return this.db.prepare('SELECT * FROM cycles WHERE customer_id IS ? ORDER BY start_time DESC LIMIT 1').get(customerId) as
      | DbCycle
      | undefined ?? null;
  }

  getLatestRecovery(customerId: string | null = null): DbRecovery | null {
    return this.db.prepare('SELECT * FROM recovery WHERE customer_id IS ? ORDER BY created_at DESC LIMIT 1').get(customerId) as
      | DbRecovery
      | undefined ?? null;
  }

  getLatestSleep(customerId: string | null = null): DbSleep | null {
    return this.db.prepare('SELECT * FROM sleep WHERE customer_id IS ? AND is_nap = 0 ORDER BY start_time DESC LIMIT 1').get(customerId) as
      | DbSleep
      | undefined ?? null;
  }

  getCyclesByDateRange(startDate: string, endDate: string, customerId: string | null = null): DbCycle[] {
    return this.db.prepare(`
      SELECT * FROM cycles WHERE customer_id IS ? AND start_time >= ? AND start_time <= ? ORDER BY start_time DESC
    `).all(customerId, startDate, endDate) as DbCycle[];
  }

  getRecoveriesByDateRange(startDate: string, endDate: string, customerId: string | null = null): DbRecovery[] {
    return this.db.prepare(`
      SELECT * FROM recovery WHERE customer_id IS ? AND created_at >= ? AND created_at <= ? ORDER BY created_at DESC
    `).all(customerId, startDate, endDate) as DbRecovery[];
  }

  getSleepsByDateRange(startDate: string, endDate: string, includeNaps = false, customerId: string | null = null): DbSleep[] {
    const query = includeNaps
      ? 'SELECT * FROM sleep WHERE customer_id IS ? AND start_time >= ? AND start_time <= ? ORDER BY start_time DESC'
      : 'SELECT * FROM sleep WHERE customer_id IS ? AND start_time >= ? AND start_time <= ? AND is_nap = 0 ORDER BY start_time DESC';
    return this.db.prepare(query).all(customerId, startDate, endDate) as DbSleep[];
  }

  getWorkoutsByDateRange(startDate: string, endDate: string, customerId: string | null = null): DbWorkout[] {
    return this.db.prepare(`
      SELECT * FROM workouts WHERE customer_id IS ? AND start_time >= ? AND start_time <= ? ORDER BY start_time DESC
    `).all(customerId, startDate, endDate) as DbWorkout[];
  }

  getRecoveryTrends(days: number, customerId: string | null = null): RecoveryTrendRow[] {
    return this.db.prepare(`
      SELECT DATE(created_at) as date, recovery_score, hrv_rmssd as hrv, resting_hr as rhr
      FROM recovery
      WHERE customer_id IS ? AND recovery_score IS NOT NULL AND created_at >= DATE('now', '-' || ? || ' days')
      ORDER BY created_at DESC
    `).all(customerId, days) as RecoveryTrendRow[];
  }

  getSleepTrends(days: number, customerId: string | null = null): SleepTrendRow[] {
    return this.db.prepare(`
      SELECT DATE(start_time) as date,
             ROUND((total_in_bed_milli - total_awake_milli) / 3600000.0, 2) as total_sleep_hours,
             sleep_performance as performance, sleep_efficiency as efficiency
      FROM sleep
      WHERE customer_id IS ? AND is_nap = 0 AND sleep_performance IS NOT NULL AND start_time >= DATE('now', '-' || ? || ' days')
      ORDER BY start_time DESC
    `).all(customerId, days) as SleepTrendRow[];
  }

  getStrainTrends(days: number, customerId: string | null = null): StrainTrendRow[] {
    return this.db.prepare(`
      SELECT DATE(start_time) as date, strain, ROUND(kilojoule / 4.184, 0) as calories
      FROM cycles
      WHERE customer_id IS ? AND strain IS NOT NULL AND start_time >= DATE('now', '-' || ? || ' days')
      ORDER BY start_time DESC
    `).all(customerId, days) as StrainTrendRow[];
  }

  // ---------- Checklist / journal ----------
  // Stored in the correctly composite-keyed *_v2 tables. customerId = null
  // (the legacy single-owner app / MCP tools) maps to customer_key = 'owner';
  // any other value is a real customer id. This keeps two different people's
  // entries for the same calendar date from colliding.

  private static customerKey(customerId: string | null): string {
    return customerId ?? 'owner';
  }

  getChecklist(date: string, customerId: string | null = null): Record<string, boolean> {
    const key = WhoopDatabase.customerKey(customerId);
    const rows = this.db.prepare('SELECT task_id, done FROM checklist_v2 WHERE customer_key = ? AND date = ?').all(key, date) as {
      task_id: string;
      done: number;
    }[];
    const result: Record<string, boolean> = {};
    for (const row of rows) result[row.task_id] = Boolean(row.done);
    return result;
  }

  setChecklistItem(date: string, taskId: string, done: boolean, customerId: string | null = null): void {
    const key = WhoopDatabase.customerKey(customerId);
    this.db.prepare(`
      INSERT INTO checklist_v2 (customer_key, date, task_id, done, updated_at)
      VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(customer_key, date, task_id) DO UPDATE SET done = excluded.done, updated_at = CURRENT_TIMESTAMP
    `).run(key, date, taskId, done ? 1 : 0);
  }

  getJournal(date: string, customerId: string | null = null): Record<string, unknown> | null {
    const key = WhoopDatabase.customerKey(customerId);
    const row = this.db.prepare('SELECT * FROM journal_v2 WHERE customer_key = ? AND date = ?').get(key, date);
    return (row as Record<string, unknown>) ?? null;
  }

  saveJournal(
    date: string,
    entry: {
      alcohol?: number | null;
      alcoholLastTime?: string | null;
      caffeineCount?: number | null;
      caffeineLastTime?: string | null;
      sauna?: number | null;
      coldExposure?: number | null;
      lateMeal?: number | null;
      screenTime?: number | null;
      meditation?: number | null;
      stretching?: number | null;
      nap?: number | null;
      sick?: number | null;
      travel?: number | null;
      mood?: number | null;
      stress?: number | null;
      notes?: string | null;
    },
    customerId: string | null = null
  ): void {
    const key = WhoopDatabase.customerKey(customerId);
    this.db.prepare(`
      INSERT INTO journal_v2 (
        customer_key, date, alcohol, alcohol_last_time, caffeine_count, caffeine_last_time, sauna, cold_exposure,
        late_meal, screen_time, meditation, stretching, nap, sick, travel, mood, stress, notes, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(customer_key, date) DO UPDATE SET
        alcohol = excluded.alcohol,
        alcohol_last_time = excluded.alcohol_last_time,
        caffeine_count = excluded.caffeine_count,
        caffeine_last_time = excluded.caffeine_last_time,
        sauna = excluded.sauna,
        cold_exposure = excluded.cold_exposure,
        late_meal = excluded.late_meal,
        screen_time = excluded.screen_time,
        meditation = excluded.meditation,
        stretching = excluded.stretching,
        nap = excluded.nap,
        sick = excluded.sick,
        travel = excluded.travel,
        mood = excluded.mood,
        stress = excluded.stress,
        notes = excluded.notes,
        updated_at = CURRENT_TIMESTAMP
    `).run(
      key,
      date,
      entry.alcohol ?? null,
      entry.alcoholLastTime ?? null,
      entry.caffeineCount ?? null,
      entry.caffeineLastTime ?? null,
      entry.sauna ?? null,
      entry.coldExposure ?? null,
      entry.lateMeal ?? null,
      entry.screenTime ?? null,
      entry.meditation ?? null,
      entry.stretching ?? null,
      entry.nap ?? null,
      entry.sick ?? null,
      entry.travel ?? null,
      entry.mood ?? null,
      entry.stress ?? null,
      entry.notes ?? null
    );
  }

  getJournalSettings(customerId: string | null = null): string[] {
    const row = this.db.prepare(`SELECT enabled_fields FROM journal_settings WHERE id = ? AND customer_id IS ?`).get(
      customerId ?? 'default',
      customerId
    ) as { enabled_fields: string } | undefined;
    if (!row) {
      if (customerId === null) return [];
      // fall back to the default set for brand-new customers
      const fallback = this.db.prepare(`SELECT enabled_fields FROM journal_settings WHERE id = 'default'`).get() as
        | { enabled_fields: string }
        | undefined;
      if (!fallback) return [];
      try {
        return JSON.parse(fallback.enabled_fields);
      } catch (e) {
        return [];
      }
    }
    try {
      return JSON.parse(row.enabled_fields);
    } catch (e) {
      return [];
    }
  }

  saveJournalSettings(enabledFields: string[], customerId: string | null = null): void {
    const id = customerId ?? 'default';
    this.db.prepare(`
      INSERT INTO journal_settings (id, enabled_fields, customer_id) VALUES (?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET enabled_fields = excluded.enabled_fields
    `).run(id, JSON.stringify(enabledFields), customerId);
  }

  close(): void {
    this.db.close();
  }
}
