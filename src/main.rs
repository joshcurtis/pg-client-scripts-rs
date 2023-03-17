use ::postgres::{Client, NoTls};


fn check_if_accounts_exists(client: &mut Client) -> bool {
    let sql = "SELECT * FROM pg_stat_user_tables WHERE relname = 'accounts';";
    let row = client.query_opt(sql, &[]).unwrap();
    return row.is_some();
}

fn create_accounts_table(client: &mut Client) {
    let sql = "
DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts(
    a_id            BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT    NOT NULL,
    balance         BIGINT NOT NULL
) WITH (FILLFACTOR = 10);
CREATE UNIQUE INDEX idempotency_key_idx ON accounts (idempotency_key);
";
    client.batch_execute(sql).unwrap();
}

fn insert_into_accounts(client: &mut Client, idem_key: &str) -> i64 {
    let opt_row = client.query_opt(
        "SELECT a_id FROM accounts WHERE idempotency_key = $1",
        &[&idem_key]
    ).unwrap();
    if opt_row.is_some() {
        return opt_row.unwrap().get(0)
    }

    let sql = "INSERT INTO accounts(idempotency_key, balance) VALUES($1, $2) RETURNING a_id";
    let bal: i64 = 0;
    return client.query_one(
        sql,
        &[&idem_key, &bal],
    )
        .unwrap()
        .get(0);
}

fn update_account_balance(client: &mut Client, a_id: i64, balance: i64) {
    client.execute(
        "UPDATE accounts SET balance = $2 WHERE a_id = $1",
        &[&a_id, &balance]
    ).unwrap();
}

fn main() {
    println!("Hello, world!");

    let mut client = Client::connect("host=localhost port=21800 user=josh dbname=postgres", NoTls).unwrap();
    if !check_if_accounts_exists(&mut client) {
        println!("Creating accounts");
        create_accounts_table(&mut client);
    }
    let a_id = insert_into_accounts(&mut client, "2y");
    //println!("Got {}", a_id);

    println!("updating balance");
    for i in 0..20 {
        update_account_balance(&mut client, a_id, i);
    }
}
