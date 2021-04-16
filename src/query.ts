import * as duckdb from 'duckdb';

export default function query(query: string, db: duckdb): Promise<any> {
    return new Promise((resolve, reject) => {
        db.all(query, function(err, res) {
            if (err) {
                reject(err);
            } {
                resolve(res);
            }
        });
    });
}
