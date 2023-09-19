import { Application } from 'bunjs';
import { Router } from 'bunjs/router';
import { v4 as uuidv4 } from 'uuid';
import { Database } from "bun:sqlite";

const db = new Database("mydb.sqlite");
db.exec('CREATE TABLE IF NOT EXISTS apiKeys (api_key STRING, client_keyspace STRING)');
db.exec('CREATE TABLE IF NOT EXISTS apiKeyUsage (api_key STRING, timestamp DATETIME, endpoint STRING)');

const router = new Router();

router.post('/generate_api_key', (req, res) => {
    const apiKey = uuidv4();
    const keyspaceName = 'ks_' + apiKey.replace(/-/g, '_');
    const stmt = db.prepare('INSERT INTO apiKeys (api_key, client_keyspace) VALUES (?, ?)');
    stmt.run(apiKey, keyspaceName);
    res.json({api_key: apiKey});
});

router.get('/get_usage_costs', (req, res) => {
    const apiKey = req.headers['API-Key'];
    if (!apiKey) {
        res.status(401).json({message: "API-Key header is missing"});
        return;
    }

    const stmt = db.prepare('SELECT COUNT(*) as count FROM apiKeyUsage WHERE api_key = ?');
    const count = stmt.get(apiKey).count;
    const cost = count * 0.001;

    res.json({api_key: apiKey, cost: cost});
});

router.post('/create_table', (req, res) => {
    const apiKey = req.headers['API-Key'];
    const keyspaceName = 'ks_' + apiKey.replace(/-/g, '_');
    const tableName = req.body.table_name;
    const columns = req.body.columns;
    const columnDefs = Object.entries(columns).map(([name, type]) => `${name} ${type}`).join(', ');
    const stmt = db.prepare(`CREATE TABLE IF NOT EXISTS ${keyspaceName}.${tableName} (${columnDefs})`);
    stmt.run();
    res.json({message: `Table ${tableName} created successfully in keyspace ${keyspaceName}.`});
});

router.get('/list_tables', (req, res) => {
    const apiKey = req.headers['API-Key'];
    const keyspaceName = 'ks_' + apiKey.replace(/-/g, '_');
    const stmt = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '${keyspaceName}.%'`);
    const tables = stmt.all().map(row => row.name.replace(`${keyspaceName}.`, ''));
    res.json({tables: tables});
});

router.post('/:table_name/insert_data', (req, res) => {
    const apiKey = req.headers['API-Key'];
    const keyspaceName = 'ks_' + apiKey.replace(/-/g, '_');
    const tableName = req.params.table_name;
    const data = req.body;
    const columns = Object.keys(data).join(', ');
    const values = Object.values(data);
    const placeholders = values.map(() => '?').join(', ');
    const stmt = db.prepare(`INSERT INTO ${keyspaceName}.${tableName} (${columns}) VALUES (${placeholders})`);
    stmt.run(values);
    res.json({message: 'Data inserted successfully.'});
});

router.get('/:table_name/query_data', (req, res) => {
    const apiKey = req.headers['API-Key'];
    const keyspaceName = 'ks_' + apiKey.replace(/-/g, '_');
    const tableName = req.params.table_name;
    const stmt = db.prepare(`SELECT * FROM ${keyspaceName}.${tableName}`);
    const data = stmt.all();
    res.json({data: data});
});

router.delete('/:table_name/delete_data', (req, res) => {
    const apiKey = req.headers['API-Key'];
    const keyspaceName = 'ks_' + apiKey.replace(/-/g, '_');
    const tableName = req.params.table_name;
    const id = req.body.id;
    const stmt = db.prepare(`DELETE FROM ${keyspaceName}.${tableName} WHERE id = ?`);
    stmt.run(id);
    res.json({message: 'Data deleted successfully.'});
});

router.put('/:table_name/update_data', (req, res) => {
    const apiKey = req.headers['API-Key'];
    const keyspaceName = 'ks_' + apiKey.replace(/-/g, '_');
    const tableName = req.params.table_name;
    const data = req.body;
    const id = data.id;
    delete data.id;
    const setClause = Object.entries(data).map(([name, value]) => `${name} = ?`).join(', ');
    const values = Object.values(data);
    const stmt = db.prepare(`UPDATE ${keyspaceName}.${tableName} SET ${setClause} WHERE id = ?`);
    stmt.run([...values, id]);
    res.json({message: 'Data updated successfully.'});
});

const app = new Application();
app.use(router.routes());
app.listen({ port: 8000 }).then(() => console.log('server started'));
