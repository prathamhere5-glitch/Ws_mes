import Database from "better-sqlite3";
import { Pool } from "pg";

export interface DB {
    exec(sql: string): Promise<void>;
    prepare(sql: string): {
        get(...params: any[]): Promise<any>;
        all(...params: any[]): Promise<any[]>;
        run(...params: any[]): Promise<{ changes: number }>;
    };
    close(): Promise<void>;
}

class SQLiteDB implements DB {
    private db: Database.Database;
    constructor(path: string) {
        this.db = new Database(path);
    }
    async exec(sql: string) {
        this.db.exec(sql);
    }
    prepare(sql: string) {
        const stmt = this.db.prepare(sql);
        return {
            get: async (...params: any[]) => stmt.get(...params),
            all: async (...params: any[]) => stmt.all(...params),
            run: async (...params: any[]) => {
                const res = stmt.run(...params);
                return { changes: res.changes };
            }
        };
    }
    async close() {
        this.db.close();
    }
}

class PostgresDB implements DB {
    private pool: Pool;
    constructor(url: string) {
        this.pool = new Pool({ connectionString: url, ssl: { rejectUnauthorized: false } });
    }
    async exec(sql: string) {
        // PostgreSQL doesn't support multiple statements in one query easily with parameters, 
        // but for schema creation it's fine.
        await this.pool.query(sql);
    }
    prepare(sql: string) {
        let pgSql = sql;
        
        // 1. Handle PRAGMA table_info(table_name)
        if (pgSql.toUpperCase().includes("PRAGMA TABLE_INFO")) {
            const match = pgSql.match(/PRAGMA TABLE_INFO\((\w+)\)/i);
            if (match) {
                const tableName = match[1];
                pgSql = `SELECT column_name as name FROM information_schema.columns WHERE table_name = '${tableName}'`;
            }
        }

        // 2. Handle sqlite_master (for fresh DB check)
        if (pgSql.toUpperCase().includes("SQLITE_MASTER")) {
            pgSql = `SELECT count(*) as count FROM information_schema.tables WHERE table_name = 'users'`;
        }

        // 3. Replace ? with $1, $2, etc.
        let count = 1;
        pgSql = pgSql.replace(/\?/g, () => `$${count++}`);
        
        // 4. Replace INSERT OR IGNORE
        if (pgSql.toUpperCase().includes("INSERT OR IGNORE")) {
            pgSql = pgSql.replace(/INSERT OR IGNORE INTO\s+(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)/i, (match, table, cols, vals) => {
                const colList = cols.split(',').map((c: string) => c.trim());
                let conflictTarget = colList[0];
                
                // Special case for composite primary keys in whatsapp_auth
                if (table.toLowerCase() === 'whatsapp_auth') {
                    conflictTarget = 'session_id, key_id';
                }
                
                return `INSERT INTO ${table} (${cols}) VALUES (${vals}) ON CONFLICT (${conflictTarget}) DO NOTHING`;
            });
        }

        // 5. Replace INSERT OR REPLACE
        if (pgSql.toUpperCase().includes("INSERT OR REPLACE")) {
            pgSql = pgSql.replace(/INSERT OR REPLACE INTO\s+(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)/i, (match, table, cols, vals) => {
                const colList = cols.split(',').map((c: string) => c.trim());
                let conflictTarget = colList[0];
                
                // Special case for composite primary keys in whatsapp_auth
                if (table.toLowerCase() === 'whatsapp_auth') {
                    conflictTarget = 'session_id, key_id';
                }
                
                const updateCols = colList.filter(c => !conflictTarget.split(',').map(tc => tc.trim()).includes(c));
                const updateSet = updateCols.map((c: string) => `${c} = EXCLUDED.${c}`).join(', ');
                return `INSERT INTO ${table} (${cols}) VALUES (${vals}) ON CONFLICT (${conflictTarget}) DO UPDATE SET ${updateSet}`;
            });
        }

        // 6. Replace AUTOINCREMENT with SERIAL (only in CREATE TABLE)
        if (pgSql.toUpperCase().includes("AUTOINCREMENT")) {
            pgSql = pgSql.replace(/INTEGER PRIMARY KEY AUTOINCREMENT/i, "SERIAL PRIMARY KEY");
        }

        return {
            get: async (...params: any[]) => {
                const res = await this.pool.query(pgSql, params);
                return res.rows[0];
            },
            all: async (...params: any[]) => {
                const res = await this.pool.query(pgSql, params);
                return res.rows;
            },
            run: async (...params: any[]) => {
                const res = await this.pool.query(pgSql, params);
                return { changes: res.rowCount || 0 };
            }
        };
    }
    async close() {
        await this.pool.end();
    }
}

export function createDB(dbPath: string): DB {
    const url = process.env.DATABASE_URL;
    if (url) {
        console.log("[Database] Using PostgreSQL (Remote)");
        return new PostgresDB(url);
    } else {
        console.log("[Database] Using SQLite (Local)");
        return new SQLiteDB(dbPath);
    }
}
