import pg from 'pg';
const { Pool } = pg;
import fs from 'fs';
import path from 'path';
import url from 'url';

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const SCHEMA_PATH = path.join(__dirname, 'schema.sql');

const BLACK_RACKS_TABLES = [
  { id: 1, name: 'Table 1', type: 'ENGLISH', hourly_rate: 300, minimum_charge: 100 },
  { id: 2, name: 'Table 2', type: 'FRENCH', hourly_rate: 420, minimum_charge: 150 }
];

let poolInstance = null;
let dbInstance = null;

/**
 * Automates query conversion from SQLite to PostgreSQL
 */
function translateQuery(sql) {
  let translated = sql.trim();

  // 1. Skip SQLite-specific PRAGMA settings
  if (translated.toUpperCase().startsWith('PRAGMA')) {
    return 'SELECT 1'; // Return safe no-op
  }

  // 2. Map transaction commands
  if (translated.toUpperCase() === 'BEGIN IMMEDIATE') {
    return 'BEGIN';
  }

  // 3. Translate SQLite INSERT OR REPLACE into Postgres INSERT ... ON CONFLICT (for settings)
  if (translated.includes('INSERT OR REPLACE INTO settings')) {
    translated = translated.replace(
      /INSERT OR REPLACE INTO settings\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i,
      (match, cols, vals) => {
        const colArray = cols.split(',').map(c => c.trim());
        const updateSet = colArray
          .filter(c => c !== 'key')
          .map(c => `${c} = EXCLUDED.${c}`)
          .join(', ');
        return `INSERT INTO settings (${cols}) VALUES (${vals}) ON CONFLICT (key) DO UPDATE SET ${updateSet}`;
      }
    );
  }

  // 4. Translate SQLite INSERT OR REPLACE into Postgres INSERT ... ON CONFLICT (for daily_players)
  if (translated.includes('INSERT OR REPLACE INTO daily_players')) {
    translated = translated.replace(
      /INSERT OR REPLACE INTO daily_players\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i,
      (match, cols, vals) => {
        const colArray = cols.split(',').map(c => c.trim());
        const updateSet = colArray
          .filter(c => c !== 'player_code' && c !== 'date')
          .map(c => `${c} = EXCLUDED.${c}`)
          .join(', ');
        return `INSERT INTO daily_players (${cols}) VALUES (${vals}) ON CONFLICT (player_code, date) DO UPDATE SET ${updateSet}`;
      }
    );
  }

  // 5. Translate schema types ( SQLite AUTOINCREMENT to Postgres SERIAL)
  translated = translated.replace(/INTEGER PRIMARY KEY AUTOINCREMENT/gi, 'SERIAL PRIMARY KEY');
  translated = translated.replace(/INTEGER PRIMARY KEY/gi, 'SERIAL PRIMARY KEY');

  // 6. Translate pragma_table_info queries for Postgres information_schema catalog
  if (translated.includes('pragma_table_info')) {
    translated = translated.replace(
      /pragma_table_info\('([^']+)'\)/g,
      "(SELECT column_name as name FROM information_schema.columns WHERE lower(table_name) = lower('$1')) as pragma_temp_alias"
    );
  }

  // 7. Convert SQLite "?" placeholders to Postgres "$1, $2, $3..." style
  let index = 1;
  translated = translated.replace(/\?/g, () => `$${index++}`);

  return translated;
}

/**
 * Neon Postgres database compatibility layer
 */
function createNeonAdapter(pool) {
  return {
    async get(sql, ...params) {
      const translatedSql = translateQuery(sql);
      const result = await pool.query(translatedSql, params);
      return result.rows[0] ?? undefined;
    },
    async all(sql, ...params) {
      const translatedSql = translateQuery(sql);
      const result = await pool.query(translatedSql, params);
      return [...result.rows];
    },
    async run(sql, ...params) {
      const translatedSql = translateQuery(sql);
      const result = await pool.query(translatedSql, params);
      return {
        lastID: Number(result.rows[0]?.id || 0),
        changes: result.rowCount ?? 0
      };
    },
    async exec(sql) {
      // Split and run multiple statement schema migrations
      const queries = sql
        .split(';')
        .map(q => q.trim())
        .filter(q => q.length > 0);

      for (const q of queries) {
        const translatedQ = translateQuery(q);
        await pool.query(translatedQ);
      }
    },
    async close() {
      await pool.end();
    }
  };
}

export async function getDB() {
  if (!dbInstance) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error('DATABASE_URL environment variable is missing. Please set this in Vercel to your Neon PostgreSQL connection string.');
    }
    poolInstance = new Pool({
      connectionString,
      ssl: { rejectUnauthorized: false } // Required for secure serverless connections to Neon
    });
    dbInstance = createNeonAdapter(poolInstance);
    console.log('📡 Connected to Neon PostgreSQL (persistent)');
  }
  return dbInstance;
}

export async function withTransaction(fn) {
  const db = await getDB();
  await db.run('BEGIN');
  try {
    const result = await fn(db);
    await db.run('COMMIT');
    return result;
  } catch (error) {
    await db.run('ROLLBACK');
    throw error;
  }
}

export async function closeDB() {
  if (dbInstance) {
    await dbInstance.close();
    dbInstance = null;
  }
}

export async function migrate() {
  console.log('🔄 Running database migrations...');
  const db = await getDB();

  try {
    const schema = fs.readFileSync(SCHEMA_PATH, 'utf-8');
    await db.exec(schema);

    await runAdditionalMigrations(db);

    const tableCount = await db.get('SELECT COUNT(*) as count FROM tables');
    if (Number(tableCount.count) === 0) {
      console.log('🌱 Seeding Black Racks tables...');
      await seedBlackRacksTables(db);
    } else {
      await configureBlackRacksParlor(db);
    }

    await syncTableNames(db);

    console.log('✅ Database migration completed');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    throw error;
  }
}

async function seedBlackRacksTables(db) {
  for (const table of BLACK_RACKS_TABLES) {
    await db.run(
      `INSERT INTO tables (id, name, type, hourly_rate, minimum_charge, status, light_on)
       VALUES (?, ?, ?, ?, ?, 'AVAILABLE', 0)`,
      table.id, table.name, table.type, table.hourly_rate, table.minimum_charge
    );
  }

  await db.run(
    `INSERT OR REPLACE INTO settings (key, value, description) VALUES (?, ?, ?)`,
    'parlor_name', 'Black Racks Snooker Club by Zaid', 'Name of the snooker parlor'
  );
  await db.run(
    `INSERT OR REPLACE INTO settings (key, value, description) VALUES (?, ?, ?)`,
    'black_racks_configured', 'true', 'Black Racks parlor setup complete'
  );

  console.log('✅ Black Racks tables seeded successfully');
}

async function configureBlackRacksParlor(db) {
  const configured = await db.get(`SELECT value FROM settings WHERE key = 'black_racks_configured'`);
  if (configured?.value === 'true') return;

  console.log('🎱 Configuring Black Racks Snooker Club...');

  await db.run('DELETE FROM sessions WHERE table_id > 2');
  await db.run('DELETE FROM tables WHERE id > 2');

  for (const table of BLACK_RACKS_TABLES) {
    const existing = await db.get('SELECT id FROM tables WHERE id = ?', table.id);
    if (existing) {
      await db.run(
        `UPDATE tables SET name = ?, type = ?, hourly_rate = ?, minimum_charge = ? WHERE id = ?`,
        table.name, table.type, table.hourly_rate, table.minimum_charge, table.id
      );
    } else {
      await db.run(
        `INSERT INTO tables (id, name, type, hourly_rate, minimum_charge, status, light_on)
         VALUES (?, ?, ?, ?, ?, 'AVAILABLE', 0)`,
        table.id, table.name, table.type, table.hourly_rate, table.minimum_charge
      );
    }
  }

  await db.run(
    `INSERT OR REPLACE INTO settings (key, value, description) VALUES (?, ?, ?)`,
    'parlor_name', 'Black Racks Snooker Club by Zaid', 'Name of the snooker parlor'
  );
  await db.run(
    `INSERT OR REPLACE INTO settings (key, value, description) VALUES (?, ?, ?)`,
    'black_racks_configured', 'true', 'Black Racks parlor setup complete'
  );

  console.log('✅ Black Racks configuration applied');
}

async function syncTableNames(db) {
  await db.run(`UPDATE tables SET name = 'Table 1' WHERE id = 1`);
  await db.run(`UPDATE tables SET name = 'Table 2' WHERE id = 2`);
}

async function runAdditionalMigrations(db) {
  console.log('🔧 Running additional migrations...');

  try {
    const nameColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('tables')
      WHERE name = 'name'
    `);

    if (Number(nameColumnExists.count) === 0) {
      console.log('📝 Adding name column to tables...');
      await db.exec('ALTER TABLE tables ADD COLUMN name TEXT');
      console.log('✅ Added name column');
    }

    const minimumChargeColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('tables')
      WHERE name = 'minimum_charge'
    `);

    if (Number(minimumChargeColumnExists.count) === 0) {
      console.log('📝 Adding minimum_charge column to tables...');
      await db.exec('ALTER TABLE tables ADD COLUMN minimum_charge INTEGER NOT NULL DEFAULT 0');
      console.log('✅ Added minimum_charge column');
    }

    const sessionColumns = [
      { name: 'player_one_name', sql: "ALTER TABLE sessions ADD COLUMN player_one_name TEXT" },
      { name: 'player_two_name', sql: "ALTER TABLE sessions ADD COLUMN player_two_name TEXT" },
      { name: 'loser', sql: "ALTER TABLE sessions ADD COLUMN loser TEXT" },
      { name: 'food_charge', sql: "ALTER TABLE sessions ADD COLUMN food_charge INTEGER NOT NULL DEFAULT 0" },
      { name: 'tip', sql: "ALTER TABLE sessions ADD COLUMN tip INTEGER NOT NULL DEFAULT 0" },
      { name: 'food_items', sql: "ALTER TABLE sessions ADD COLUMN food_items TEXT" },
      { name: 'payer_name', sql: "ALTER TABLE sessions ADD COLUMN payer_name TEXT" }
    ];

    for (const column of sessionColumns) {
      const result = await db.get(`
        SELECT COUNT(*) as count
        FROM pragma_table_info('sessions')
        WHERE name = ?
      `, column.name);
      if (Number(result.count) === 0) {
        console.log(`📝 Adding ${column.name} column to sessions...`);
        await db.exec(column.sql);
        console.log(`✅ Added ${column.name} column`);
      }
    }

    const loyaltyColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('customers')
      WHERE name = 'loyalty_points'
    `);

    if (Number(loyaltyColumnExists.count) === 0) {
      console.log('📝 Adding loyalty_points column to customers table...');
      await db.exec('ALTER TABLE customers ADD COLUMN loyalty_points INTEGER DEFAULT 0');
      console.log('✅ Added loyalty_points column');
    }

    const expiryColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('customers')
      WHERE name = 'membership_expiry_date'
    `);

    if (Number(expiryColumnExists.count) === 0) {
      console.log('📝 Adding membership_expiry_date column to customers table...');
      await db.exec('ALTER TABLE customers ADD COLUMN membership_expiry_date INTEGER');
      console.log('✅ Added membership_expiry_date column');
    }

    const statusColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('customers')
      WHERE name = 'membership_status'
    `);

    if (Number(statusColumnExists.count) === 0) {
      console.log('📝 Adding membership_status column to customers table...');
      await db.exec(`ALTER TABLE customers ADD COLUMN membership_status TEXT DEFAULT 'ACTIVE'`);
      console.log('✅ Added membership_status column');
    }

    const dobColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('customers')
      WHERE name = 'date_of_birth'
    `);

    if (Number(dobColumnExists.count) === 0) {
      console.log('📝 Adding date_of_birth column to customers table...');
      await db.exec('ALTER TABLE customers ADD COLUMN date_of_birth INTEGER');
      console.log('✅ Added date_of_birth column');
    }

    const addressColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('customers')
      WHERE name = 'address'
    `);

    if (Number(addressColumnExists.count) === 0) {
      console.log('📝 Adding address column to customers table...');
      await db.exec('ALTER TABLE customers ADD COLUMN address TEXT');
      console.log('✅ Added address column');
    }

    const emergencyColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('customers')
      WHERE name = 'emergency_contact'
    `);

    if (Number(emergencyColumnExists.count) === 0) {
      console.log('📝 Adding emergency_contact column to customers table...');
      await db.exec('ALTER TABLE customers ADD COLUMN emergency_contact TEXT');
      console.log('✅ Added emergency_contact column');
    }

    const idCardColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('customers')
      WHERE name = 'id_card_number'
    `);

    if (Number(idCardColumnExists.count) === 0) {
      console.log('📝 Adding id_card_number column to customers table...');
      await db.exec('ALTER TABLE customers ADD COLUMN id_card_number TEXT');
      console.log('✅ Added id_card_number column');
    }

    const photoColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('customers')
      WHERE name = 'photo_url'
    `);

    if (Number(photoColumnExists.count) === 0) {
      console.log('📝 Adding photo_url column to customers table...');
      await db.exec('ALTER TABLE customers ADD COLUMN photo_url TEXT');
      console.log('✅ Added photo_url column');
    }

    const membershipStartColumnExists = await db.get(`
      SELECT COUNT(*) as count
      FROM pragma_table_info('customers')
      WHERE name = 'membership_start_date'
    `);

    if (Number(membershipStartColumnExists.count) === 0) {
      console.log('Adding membership_start_date column to customers table...');
      await db.exec('ALTER TABLE customers ADD COLUMN membership_start_date INTEGER');
    }

    await db.exec(`
      CREATE TABLE IF NOT EXISTS daily_players (
        id SERIAL PRIMARY KEY,
        player_code TEXT NOT NULL,
        name TEXT NOT NULL,
        phone TEXT,
        sessions_count INTEGER NOT NULL DEFAULT 0,
        total_spent INTEGER NOT NULL DEFAULT 0,
        date TEXT NOT NULL,
        last_seen INTEGER NOT NULL,
        UNIQUE(player_code, date)
      )
    `);
    
    await db.exec('CREATE INDEX IF NOT EXISTS idx_daily_players_date ON daily_players(date)');
    await db.exec('CREATE INDEX IF NOT EXISTS idx_daily_players_name ON daily_players(name)');

    await db.run(`DELETE FROM user_sessions WHERE user_id IN (SELECT id FROM users WHERE role = 'employee')`);
    await db.run(`DELETE FROM users WHERE role = 'employee'`);

  } catch (error) {
    console.error('❌ Additional migrations failed:', error);
  }
}

export async function backupDatabase() {
  throw new Error('Database backup is not supported automatically from the app with Neon. Please use the Neon dashboard back-ups.');
}