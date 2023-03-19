use std::arch::aarch64::float32x2_t;
use std::collections::{BTreeMap, HashMap};
use std::thread;
use ::postgres::{Client, NoTls};
use postgres_types::{ToSql, FromSql};
use postgres::{Error, Row};


// derive from sql

struct HeapPageItem {
    lp_flags: i32,
    t_xmin: Option<String>,
    t_xmax: Option<String>,
}


fn get_num_pages(client: &mut Client, relname: &str) -> i32 {
    client.execute(format!("ANALYZE {}", relname).as_str(), &[]).unwrap();
    let sql = "SELECT relpages::text::int FROM pg_class WHERE relname = $1";
    let result = client.query_one(sql, &[&relname]);
    match result {
        Ok(row) => {return row.get(0) }
        Err(err) => {
            println!("Error: {}", err);
            panic!("{}", err)
        }
    }
}

fn check_if_accounts_exists(client: &mut Client) -> bool {
    let sql = "SELECT * FROM pg_stat_user_tables WHERE relname = 'accounts';";
    let row = client.query_opt(sql, &[]).unwrap();
    row.is_some()
}

fn heap_page_items(client: &mut Client, relname: &str, blkno: i32) -> Vec<HeapPageItem> {
    // lol use string formatting because the rust crate can't serialize to an Int8?
    let sql = format!("SELECT lp_flags::int, t_xmin::text, t_xmax::text FROM heap_page_items(get_raw_page($1, {}))", blkno);
    let rows = client.query(&sql, &[&relname])
        .unwrap();
    rows.into_iter().map(|row| HeapPageItem {
        lp_flags: row.get(0),
        t_xmin: row.get(1),
        t_xmax: row.get(2),
    }).collect::<Vec<HeapPageItem>>()
}


fn reset_accounts_table(client: &mut Client) {
    let sql = "
DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts(
    a_id            BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT    NOT NULL,
    balance         BIGINT NOT NULL CHECK (balance >= 0)
) WITH (FILLFACTOR = 10);
CREATE UNIQUE INDEX idempotency_key_idx ON accounts (idempotency_key);
";
    client.batch_execute(sql).unwrap();
}

fn insert_into_accounts(client: &mut Client, idem_key: &str) -> i64 {
    let opt_row = client.query_opt(
        "SELECT a_id FROM accounts WHERE idempotency_key = $1",
        &[&idem_key],
    ).unwrap();
    if let Some(..) = opt_row {
        return opt_row.unwrap().get(0);
    }

    let sql = "INSERT INTO accounts(idempotency_key, balance) VALUES($1, $2) RETURNING a_id";
    let bal: i64 = 0;
    client.query_one(
        sql,
        &[&idem_key, &bal],
    )
        .unwrap()
        .get(0)
}

fn update_account_balance(client: &mut Client, a_id: i64, balance: i64) {
    client.execute(
        "UPDATE accounts SET balance = $2 WHERE a_id = $1",
        &[&a_id, &balance],
    ).unwrap();
}

fn main() {
    println!("Hello, world!");

    let mut client = connect_to_local();
    println!("Creating accounts");
    reset_accounts_table(&mut client);
    let a_id = insert_into_accounts(&mut client, "2y");
    //println!("Got {}", a_id);

    let tuples_to_fill_page = 160;

    println!("updating balance");
    // start from 1 b/c we already did an insert
    for i in 1..tuples_to_fill_page {
        update_account_balance(&mut client, a_id, i);
        let heap_page_items = heap_page_items(&mut client, "accounts", 0);
        // group heap_page_items by item.lp_flags
        let lp_flag_to_count: BTreeMap<i32, i32> = heap_page_items.iter()
            .fold(BTreeMap::new(),|mut lp_flag_to_count, item| {
                let count = lp_flag_to_count.entry(item.lp_flags).or_insert(0);
                *count += 1;
                lp_flag_to_count
            });
        println!("--------");
        lp_flag_to_count.iter().for_each(|(lp_flag, count)| {
            println!("lp_flag: {} count: {}", lp_flag, count);
        });


        let lp_flags2_count = heap_page_items.iter().filter(|item| item.lp_flags == 2).count();
        if lp_flags2_count > 0 {
            println!("lp_flags2_count: {} at {} tuples", lp_flags2_count, i);
            assert_eq!(i, 16);
            break
        }
    }

    println!("----------------------------------------------------------------");
    println!("new experiment");
    reset_accounts_table(&mut client);
    let a_id = insert_into_accounts(&mut client, "2y");

    let mut client2 = connect_to_local();
    let mut tx = client2.transaction().unwrap();
    tx.execute("SELECT * FROM accounts;", &[]).unwrap();

    for i in 1..20000 {
        update_account_balance(&mut client, a_id, i);
        // pages aren't necessarily 1:1 with blocks, but we can assume they are 1:1 here
        let n_pages = get_num_pages(&mut client, "accounts");
        let heap_page_items = heap_page_items(&mut client, "accounts", n_pages - 1);
        let lp_flags2_count = heap_page_items.iter().filter(|item| item.lp_flags == 2).count();
        if lp_flags2_count > 0 {
            println!("lp_flags2_count: {} at {} tuples", lp_flags2_count, i);
            return;
        }
    }
    let n_pages = get_num_pages(&mut client, "accounts");
    let heap_page_items = heap_page_items(&mut client, "accounts", n_pages - 1);
    let lp_flag_to_count: BTreeMap<i32, i32> = heap_page_items.iter()
        .fold(BTreeMap::new(),|mut lp_flag_to_count, item| {
            let count = lp_flag_to_count.entry(item.lp_flags).or_insert(0);
            *count += 1;
            lp_flag_to_count
        });
    println!("num pages: {}", n_pages);
    lp_flag_to_count.iter().for_each(|(lp_flag, count)| {
        println!("(lp_flag: {} count: {}", lp_flag, count);
    });

    wait_for_enter()
}


fn wait_for_enter() {
    loop {
        println!("Press enter to continue");
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        if line == "\n" {
            break;
        }
    }
}

fn connect_to_local() -> Client {
    Client::connect("host=localhost port=21800 user=josh dbname=postgres", NoTls).unwrap()
}
