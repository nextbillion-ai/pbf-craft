use std::mem;
use std::vec::IntoIter;

use postgres::{Client, Portal, Row, Transaction};

pub struct PagingCursor<'client> {
    transaction: Option<Transaction<'client>>,
    portal: Portal,
    limit: usize,
    eof: bool,
    cache: IntoIter<Row>,
}

impl<'client> Iterator for PagingCursor<'client> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let mut the_next = self.cache.next();
        if the_next.is_none() && !self.eof {
            let rows = self.fetch_next().expect("Failed to fetch");
            self.cache = rows.into_iter();
            the_next = self.cache.next();
        }
        the_next
    }
}

impl<'client> PagingCursor<'client> {
    pub fn new(sql: &str, client: &'client mut Client) -> PagingCursor<'client> {
        let mut transaction = client.transaction().unwrap();
        let portal = transaction.bind(sql, &[]).unwrap();
        let cursor = Self {
            transaction: Some(transaction),
            portal,
            limit: 32000,
            eof: false,
            cache: Vec::with_capacity(0).into_iter(),
        };
        return cursor;
    }

    fn fetch_next(&mut self) -> anyhow::Result<Vec<Row>> {
        if let Some(trans) = &mut self.transaction {
            let rows = trans.query_portal(&self.portal, self.limit as i32)?;
            if rows.len() < self.limit {
                let trans = mem::replace(&mut self.transaction, None);
                trans.unwrap().commit()?;
                self.eof = true;
            }
            return Ok(rows);
        }
        Err(anyhow!("something wrong"))
    }
}
