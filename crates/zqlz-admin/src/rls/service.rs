//! Row Level Security (RLS) policy management service implementation
//!
//! Provides functionality for creating, enabling, and managing Row Level Security
//! policies in PostgreSQL. RLS allows tables to have policies that restrict which
//! rows can be returned by queries or which rows can be modified.
//!
//! Note: RLS is a PostgreSQL-specific feature. Other databases have different
//! approaches to row-level security.

use serde::{Deserialize, Serialize};

/// Commands that a policy can apply to
///
/// PostgreSQL RLS policies can be restricted to specific SQL commands.
/// This enum represents the supported command types.
///
/// # Examples
///
/// ```
/// use zqlz_admin::PolicyCommand;
///
/// let cmd = PolicyCommand::Select;
/// assert_eq!(cmd.as_sql(), "SELECT");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PolicyCommand {
    /// SELECT queries
    Select,
    /// INSERT statements
    Insert,
    /// UPDATE statements
    Update,
    /// DELETE statements
    Delete,
    /// All commands (default)
    #[default]
    All,
}

impl PolicyCommand {
    /// Convert to SQL command keyword
    pub fn as_sql(&self) -> &'static str {
        match self {
            PolicyCommand::Select => "SELECT",
            PolicyCommand::Insert => "INSERT",
            PolicyCommand::Update => "UPDATE",
            PolicyCommand::Delete => "DELETE",
            PolicyCommand::All => "ALL",
        }
    }

    /// Check if this is a read-only command
    pub fn is_read_only(&self) -> bool {
        matches!(self, PolicyCommand::Select)
    }

    /// Check if this is a write command
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            PolicyCommand::Insert | PolicyCommand::Update | PolicyCommand::Delete
        )
    }
}

/// Policy type determining how the policy applies
///
/// PERMISSIVE policies allow access (combined with OR).
/// RESTRICTIVE policies restrict access (combined with AND).
///
/// # Examples
///
/// ```
/// use zqlz_admin::PolicyType;
///
/// let ptype = PolicyType::Permissive;
/// assert_eq!(ptype.as_sql(), "PERMISSIVE");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyType {
    /// Permissive - policies are combined with OR
    Permissive,
    /// Restrictive - policies are combined with AND
    Restrictive,
}

impl PolicyType {
    /// Convert to SQL keyword
    pub fn as_sql(&self) -> &'static str {
        match self {
            PolicyType::Permissive => "PERMISSIVE",
            PolicyType::Restrictive => "RESTRICTIVE",
        }
    }
}

impl Default for PolicyType {
    fn default() -> Self {
        PolicyType::Permissive
    }
}

/// Specification for creating an RLS policy
///
/// # Examples
///
/// ```
/// use zqlz_admin::{RlsPolicy, PolicyCommand};
///
/// let policy = RlsPolicy::new("user_data_policy", "users")
///     .with_command(PolicyCommand::Select)
///     .with_using("user_id = current_user_id()");
/// assert_eq!(policy.name(), "user_data_policy");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsPolicy {
    name: String,
    table: String,
    schema: Option<String>,
    command: PolicyCommand,
    policy_type: PolicyType,
    roles: Vec<String>,
    using_expr: Option<String>,
    check_expr: Option<String>,
}

impl RlsPolicy {
    /// Create a new RLS policy specification
    ///
    /// # Arguments
    /// * `name` - Policy name (must be unique per table)
    /// * `table` - Table the policy applies to
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            schema: None,
            command: PolicyCommand::All,
            policy_type: PolicyType::Permissive,
            roles: Vec::new(),
            using_expr: None,
            check_expr: None,
        }
    }

    /// Set the schema for the table
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the command this policy applies to
    pub fn with_command(mut self, command: PolicyCommand) -> Self {
        self.command = command;
        self
    }

    /// Set the policy type (PERMISSIVE or RESTRICTIVE)
    pub fn with_policy_type(mut self, policy_type: PolicyType) -> Self {
        self.policy_type = policy_type;
        self
    }

    /// Set roles this policy applies to
    ///
    /// If empty, applies to PUBLIC (all roles).
    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }

    /// Add a single role this policy applies to
    pub fn for_role(mut self, role: impl Into<String>) -> Self {
        self.roles.push(role.into());
        self
    }

    /// Set the USING expression
    ///
    /// The USING expression is evaluated for existing rows (SELECT, UPDATE, DELETE).
    pub fn with_using(mut self, expr: impl Into<String>) -> Self {
        self.using_expr = Some(expr.into());
        self
    }

    /// Set the WITH CHECK expression
    ///
    /// The CHECK expression is evaluated for new rows (INSERT, UPDATE).
    pub fn with_check(mut self, expr: impl Into<String>) -> Self {
        self.check_expr = Some(expr.into());
        self
    }

    /// Get the policy name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the table name
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Get the schema (if set)
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Get the qualified table name (schema.table or just table)
    pub fn qualified_table(&self) -> String {
        match &self.schema {
            Some(s) => format!("{}.{}", s, self.table),
            None => self.table.clone(),
        }
    }

    /// Get the command this policy applies to
    pub fn command(&self) -> PolicyCommand {
        self.command
    }

    /// Get the policy type
    pub fn policy_type(&self) -> PolicyType {
        self.policy_type
    }

    /// Get the roles this policy applies to
    pub fn roles(&self) -> &[String] {
        &self.roles
    }

    /// Get the USING expression
    pub fn using_expr(&self) -> Option<&str> {
        self.using_expr.as_deref()
    }

    /// Get the WITH CHECK expression
    pub fn check_expr(&self) -> Option<&str> {
        self.check_expr.as_deref()
    }
}

/// Error type for RLS operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RlsError {
    /// Policy name is empty
    EmptyName,
    /// Table name is empty
    EmptyTable,
    /// No USING or CHECK expression provided
    NoExpression,
    /// INSERT command requires WITH CHECK expression
    InsertRequiresCheck,
    /// SELECT/DELETE commands don't use WITH CHECK
    SelectDeleteNoCheck,
    /// Invalid expression (empty)
    EmptyExpression,
    /// Feature not supported
    NotSupported(String),
}

impl std::fmt::Display for RlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RlsError::EmptyName => write!(f, "Policy name cannot be empty"),
            RlsError::EmptyTable => write!(f, "Table name cannot be empty"),
            RlsError::NoExpression => {
                write!(f, "At least USING or WITH CHECK expression is required")
            }
            RlsError::InsertRequiresCheck => {
                write!(f, "INSERT policies require a WITH CHECK expression")
            }
            RlsError::SelectDeleteNoCheck => {
                write!(f, "SELECT and DELETE policies cannot use WITH CHECK")
            }
            RlsError::EmptyExpression => write!(f, "Expression cannot be empty"),
            RlsError::NotSupported(feature) => {
                write!(f, "{} is not supported", feature)
            }
        }
    }
}

impl std::error::Error for RlsError {}

/// Row Level Security management service
///
/// Generates DDL for PostgreSQL Row Level Security policies.
/// RLS is a PostgreSQL-specific feature that restricts which rows
/// can be returned or modified based on security policies.
///
/// # Examples
///
/// ```
/// use zqlz_admin::{RlsService, RlsPolicy, PolicyCommand};
///
/// let service = RlsService::new();
///
/// // Create a policy that only allows users to see their own rows
/// let policy = RlsPolicy::new("own_rows", "user_data")
///     .with_command(PolicyCommand::Select)
///     .with_using("user_id = current_user_id()");
///
/// let sql = service.build_create_policy(&policy).unwrap();
/// assert!(sql.contains("CREATE POLICY"));
/// ```
pub struct RlsService;

impl RlsService {
    /// Create a new RLS service
    pub fn new() -> Self {
        Self
    }

    /// Validate an RLS policy specification
    pub fn validate(&self, policy: &RlsPolicy) -> Result<(), RlsError> {
        if policy.name.trim().is_empty() {
            return Err(RlsError::EmptyName);
        }

        if policy.table.trim().is_empty() {
            return Err(RlsError::EmptyTable);
        }

        // Validate expression requirements based on command
        match policy.command {
            PolicyCommand::Insert => {
                // INSERT requires WITH CHECK, USING is not used
                if policy.check_expr.is_none() {
                    return Err(RlsError::InsertRequiresCheck);
                }
            }
            PolicyCommand::Select | PolicyCommand::Delete => {
                // SELECT/DELETE use USING only
                if policy.using_expr.is_none() {
                    return Err(RlsError::NoExpression);
                }
                if policy.check_expr.is_some() {
                    return Err(RlsError::SelectDeleteNoCheck);
                }
            }
            PolicyCommand::Update | PolicyCommand::All => {
                // UPDATE and ALL can use both USING and CHECK
                if policy.using_expr.is_none() && policy.check_expr.is_none() {
                    return Err(RlsError::NoExpression);
                }
            }
        }

        // Validate expressions are not empty strings
        if let Some(expr) = &policy.using_expr {
            if expr.trim().is_empty() {
                return Err(RlsError::EmptyExpression);
            }
        }
        if let Some(expr) = &policy.check_expr {
            if expr.trim().is_empty() {
                return Err(RlsError::EmptyExpression);
            }
        }

        Ok(())
    }

    /// Build an ALTER TABLE ... ENABLE ROW LEVEL SECURITY statement
    ///
    /// RLS must be enabled on a table before policies take effect.
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `schema` - Optional schema name
    pub fn build_enable_rls(&self, table: &str, schema: Option<&str>) -> String {
        let qualified = self.qualified_table(table, schema);
        format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY", qualified)
    }

    /// Build an ALTER TABLE ... DISABLE ROW LEVEL SECURITY statement
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `schema` - Optional schema name
    pub fn build_disable_rls(&self, table: &str, schema: Option<&str>) -> String {
        let qualified = self.qualified_table(table, schema);
        format!("ALTER TABLE {} DISABLE ROW LEVEL SECURITY", qualified)
    }

    /// Build an ALTER TABLE ... FORCE ROW LEVEL SECURITY statement
    ///
    /// By default, table owners bypass RLS. FORCE makes owners subject to policies too.
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `schema` - Optional schema name
    pub fn build_force_rls(&self, table: &str, schema: Option<&str>) -> String {
        let qualified = self.qualified_table(table, schema);
        format!("ALTER TABLE {} FORCE ROW LEVEL SECURITY", qualified)
    }

    /// Build an ALTER TABLE ... NO FORCE ROW LEVEL SECURITY statement
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `schema` - Optional schema name
    pub fn build_no_force_rls(&self, table: &str, schema: Option<&str>) -> String {
        let qualified = self.qualified_table(table, schema);
        format!("ALTER TABLE {} NO FORCE ROW LEVEL SECURITY", qualified)
    }

    /// Build a CREATE POLICY statement
    ///
    /// # Arguments
    /// * `policy` - Policy specification
    ///
    /// # Returns
    /// * `Ok(String)` - The CREATE POLICY statement
    /// * `Err(RlsError)` - If validation fails
    pub fn build_create_policy(&self, policy: &RlsPolicy) -> Result<String, RlsError> {
        self.validate(policy)?;

        let quoted_name = self.quote_identifier(&policy.name);
        let qualified_table = self.qualified_table(&policy.table, policy.schema.as_deref());

        let mut sql = format!("CREATE POLICY {} ON {}", quoted_name, qualified_table);

        // Add AS PERMISSIVE/RESTRICTIVE (only if not default)
        if policy.policy_type != PolicyType::Permissive {
            sql.push_str(&format!(" AS {}", policy.policy_type.as_sql()));
        }

        // Add FOR command (only if not ALL)
        if policy.command != PolicyCommand::All {
            sql.push_str(&format!(" FOR {}", policy.command.as_sql()));
        }

        // Add TO roles
        if policy.roles.is_empty() {
            sql.push_str(" TO PUBLIC");
        } else {
            let roles: Vec<String> = policy
                .roles
                .iter()
                .map(|r| self.quote_identifier(r))
                .collect();
            sql.push_str(&format!(" TO {}", roles.join(", ")));
        }

        // Add USING expression
        if let Some(using) = &policy.using_expr {
            sql.push_str(&format!(" USING ({})", using));
        }

        // Add WITH CHECK expression
        if let Some(check) = &policy.check_expr {
            sql.push_str(&format!(" WITH CHECK ({})", check));
        }

        Ok(sql)
    }

    /// Build a DROP POLICY statement
    ///
    /// # Arguments
    /// * `name` - Policy name
    /// * `table` - Table name
    /// * `schema` - Optional schema name
    /// * `if_exists` - Add IF EXISTS clause
    pub fn build_drop_policy(
        &self,
        name: &str,
        table: &str,
        schema: Option<&str>,
        if_exists: bool,
    ) -> String {
        let quoted_name = self.quote_identifier(name);
        let qualified_table = self.qualified_table(table, schema);
        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };

        format!(
            "DROP POLICY {}{} ON {}",
            if_exists_clause, quoted_name, qualified_table
        )
    }

    /// Build an ALTER POLICY statement to rename a policy
    ///
    /// # Arguments
    /// * `old_name` - Current policy name
    /// * `new_name` - New policy name
    /// * `table` - Table name
    /// * `schema` - Optional schema name
    pub fn build_rename_policy(
        &self,
        old_name: &str,
        new_name: &str,
        table: &str,
        schema: Option<&str>,
    ) -> String {
        let quoted_old = self.quote_identifier(old_name);
        let quoted_new = self.quote_identifier(new_name);
        let qualified_table = self.qualified_table(table, schema);

        format!(
            "ALTER POLICY {} ON {} RENAME TO {}",
            quoted_old, qualified_table, quoted_new
        )
    }

    /// Build an ALTER POLICY statement to change roles
    ///
    /// # Arguments
    /// * `name` - Policy name
    /// * `table` - Table name
    /// * `schema` - Optional schema name
    /// * `roles` - New roles (empty for PUBLIC)
    pub fn build_alter_policy_roles(
        &self,
        name: &str,
        table: &str,
        schema: Option<&str>,
        roles: &[String],
    ) -> String {
        let quoted_name = self.quote_identifier(name);
        let qualified_table = self.qualified_table(table, schema);

        let roles_clause = if roles.is_empty() {
            "PUBLIC".to_string()
        } else {
            roles
                .iter()
                .map(|r| self.quote_identifier(r))
                .collect::<Vec<_>>()
                .join(", ")
        };

        format!(
            "ALTER POLICY {} ON {} TO {}",
            quoted_name, qualified_table, roles_clause
        )
    }

    /// Build an ALTER POLICY statement to change USING expression
    ///
    /// # Arguments
    /// * `name` - Policy name
    /// * `table` - Table name
    /// * `schema` - Optional schema name
    /// * `using_expr` - New USING expression (None to remove)
    pub fn build_alter_policy_using(
        &self,
        name: &str,
        table: &str,
        schema: Option<&str>,
        using_expr: Option<&str>,
    ) -> String {
        let quoted_name = self.quote_identifier(name);
        let qualified_table = self.qualified_table(table, schema);

        match using_expr {
            Some(expr) => format!(
                "ALTER POLICY {} ON {} USING ({})",
                quoted_name, qualified_table, expr
            ),
            None => format!(
                "ALTER POLICY {} ON {} USING (true)",
                quoted_name, qualified_table
            ),
        }
    }

    /// Build an ALTER POLICY statement to change WITH CHECK expression
    ///
    /// # Arguments
    /// * `name` - Policy name
    /// * `table` - Table name
    /// * `schema` - Optional schema name
    /// * `check_expr` - New WITH CHECK expression (None to remove)
    pub fn build_alter_policy_check(
        &self,
        name: &str,
        table: &str,
        schema: Option<&str>,
        check_expr: Option<&str>,
    ) -> String {
        let quoted_name = self.quote_identifier(name);
        let qualified_table = self.qualified_table(table, schema);

        match check_expr {
            Some(expr) => format!(
                "ALTER POLICY {} ON {} WITH CHECK ({})",
                quoted_name, qualified_table, expr
            ),
            None => format!(
                "ALTER POLICY {} ON {} WITH CHECK (true)",
                quoted_name, qualified_table
            ),
        }
    }

    /// Build a query to check if RLS is enabled on a table
    pub fn build_check_rls_enabled_query(&self, table: &str, schema: Option<&str>) -> String {
        let schema_clause = match schema {
            Some(s) => format!("n.nspname = '{}' AND ", s.replace('\'', "''")),
            None => String::new(),
        };

        format!(
            "SELECT c.relrowsecurity, c.relforcerowsecurity \
             FROM pg_class c \
             JOIN pg_namespace n ON c.relnamespace = n.oid \
             WHERE {}c.relname = '{}'",
            schema_clause,
            table.replace('\'', "''")
        )
    }

    /// Build a query to list all policies on a table
    pub fn build_list_policies_query(&self, table: &str, schema: Option<&str>) -> String {
        let schema_clause = match schema {
            Some(s) => format!("n.nspname = '{}' AND ", s.replace('\'', "''")),
            None => String::new(),
        };

        format!(
            "SELECT pol.polname AS name, \
                    CASE pol.polcmd \
                        WHEN 'r' THEN 'SELECT' \
                        WHEN 'a' THEN 'INSERT' \
                        WHEN 'w' THEN 'UPDATE' \
                        WHEN 'd' THEN 'DELETE' \
                        WHEN '*' THEN 'ALL' \
                    END AS command, \
                    CASE WHEN pol.polpermissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END AS type, \
                    pg_get_expr(pol.polqual, pol.polrelid) AS using_expr, \
                    pg_get_expr(pol.polwithcheck, pol.polrelid) AS check_expr, \
                    ARRAY(SELECT rolname FROM pg_roles WHERE oid = ANY(pol.polroles)) AS roles \
             FROM pg_policy pol \
             JOIN pg_class c ON pol.polrelid = c.oid \
             JOIN pg_namespace n ON c.relnamespace = n.oid \
             WHERE {}c.relname = '{}' \
             ORDER BY pol.polname",
            schema_clause,
            table.replace('\'', "''")
        )
    }

    /// Build a query to list all tables with RLS enabled
    pub fn build_list_rls_tables_query(&self, schema: Option<&str>) -> String {
        match schema {
            Some(s) => format!(
                "SELECT n.nspname AS schema, c.relname AS table, \
                        c.relrowsecurity AS rls_enabled, \
                        c.relforcerowsecurity AS rls_forced \
                 FROM pg_class c \
                 JOIN pg_namespace n ON c.relnamespace = n.oid \
                 WHERE c.relkind = 'r' AND n.nspname = '{}' \
                 AND c.relrowsecurity = true \
                 ORDER BY n.nspname, c.relname",
                s.replace('\'', "''")
            ),
            None => "SELECT n.nspname AS schema, c.relname AS table, \
                            c.relrowsecurity AS rls_enabled, \
                            c.relforcerowsecurity AS rls_forced \
                     FROM pg_class c \
                     JOIN pg_namespace n ON c.relnamespace = n.oid \
                     WHERE c.relkind = 'r' \
                     AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
                     AND c.relrowsecurity = true \
                     ORDER BY n.nspname, c.relname"
                .to_string(),
        }
    }

    /// Quote an identifier if needed
    fn quote_identifier(&self, name: &str) -> String {
        if Self::needs_quoting(name) {
            format!("\"{}\"", name.replace('"', "\"\""))
        } else {
            name.to_string()
        }
    }

    /// Build qualified table name
    fn qualified_table(&self, table: &str, schema: Option<&str>) -> String {
        match schema {
            Some(s) => format!(
                "{}.{}",
                self.quote_identifier(s),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        }
    }

    /// Check if an identifier needs quoting
    fn needs_quoting(name: &str) -> bool {
        if name.is_empty() {
            return true;
        }
        let first = name.chars().next().unwrap();
        if !first.is_ascii_alphabetic() && first != '_' {
            return true;
        }
        name.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_')
            || RESERVED_KEYWORDS.contains(&name.to_uppercase().as_str())
    }
}

impl Default for RlsService {
    fn default() -> Self {
        Self::new()
    }
}

static RESERVED_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "DROP",
    "ALTER",
    "TABLE",
    "VIEW",
    "INDEX",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "AS",
    "ON",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "FULL",
    "ORDER",
    "BY",
    "GROUP",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "UNION",
    "ALL",
    "DISTINCT",
    "INTO",
    "VALUES",
    "SET",
    "DEFAULT",
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "CONSTRAINT",
    "UNIQUE",
    "CHECK",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "IF",
    "EXISTS",
    "IN",
    "BETWEEN",
    "LIKE",
    "IS",
    "USER",
    "ROLE",
    "GRANT",
    "REVOKE",
    "SCHEMA",
    "DATABASE",
    "PUBLIC",
    "POLICY",
    "USING",
    "FORCE",
];
