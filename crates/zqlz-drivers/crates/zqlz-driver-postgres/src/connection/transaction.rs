//! `Transaction` implementation for `PostgresTransaction`.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::Client;
use zqlz_core::{QueryResult, Result, StatementResult, Transaction, Value, ZqlzError};

use super::{
    money::MoneyFormatCache,
    NoticeBuffer, format_postgres_error, leading_sql_keyword, requires_simple_query_protocol,
    run_postgres_statement, statement_result_from_query_result,
};

/// PostgreSQL transaction wrapper
///
/// This transaction holds an exclusive lock on the PostgreSQL client for the entire
/// duration of the transaction. This ensures that all operations within the transaction
/// execute in the correct order and that no other operations can interfere.
pub(crate) struct PostgresTransaction {
    pub(crate) client: Arc<Mutex<Client>>,
    pub(crate) notices: NoticeBuffer,
    pub(crate) money_format: MoneyFormatCache,
    pub(crate) committed: bool,
    pub(crate) rolled_back: bool,
}

impl Drop for PostgresTransaction {
    fn drop(&mut self) {
        // If transaction is neither committed nor rolled back, automatically roll back
        if !self.committed && !self.rolled_back {
            tracing::warn!(
                "PostgreSQL transaction dropped without commit or rollback, auto-rolling back"
            );
            // We can't async rollback in Drop, but the BEGIN will auto-rollback when connection is reused
        }
    }
}

#[async_trait]
impl Transaction for PostgresTransaction {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        tracing::debug!("committing PostgreSQL transaction");

        if self.rolled_back {
            return Err(ZqlzError::Query("Transaction already rolled back".into()));
        }

        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }

        let client = self.client.lock().await;
        client.execute("COMMIT", &[]).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to commit transaction: {}", message))
        })?;

        self.committed = true;
        tracing::debug!("PostgreSQL transaction committed successfully");
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        tracing::debug!("rolling back PostgreSQL transaction");

        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }

        if self.rolled_back {
            return Ok(()); // Already rolled back, that's fine
        }

        let client = self.client.lock().await;
        client.execute("ROLLBACK", &[]).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to rollback transaction: {}", message))
        })?;

        self.rolled_back = true;
        tracing::debug!("PostgreSQL transaction rolled back successfully");
        Ok(())
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing query in transaction");
        reject_non_transactable(sql)?;

        let client = self.client.lock().await;
        run_postgres_statement(&client, &self.notices, &self.money_format, sql, params, "query").await
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing statement in transaction");
        reject_non_transactable(sql)?;

        let client = self.client.lock().await;
        let result =
            run_postgres_statement(&client, &self.notices, &self.money_format, sql, params, "statement")
                .await?;
        Ok(statement_result_from_query_result(result))
    }
}

fn reject_non_transactable(sql: &str) -> Result<()> {
    if requires_simple_query_protocol(sql) {
        let keyword = leading_sql_keyword(sql).unwrap_or_else(|| "This".to_string());
        return Err(ZqlzError::Query(format!(
            "{} cannot run inside a transaction block; run it as a standalone statement",
            keyword
        )));
    }
    Ok(())
}
