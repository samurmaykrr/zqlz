//! User management service implementation
//!
//! Provides functionality for creating, altering, and dropping database users
//! across different database dialects.

use serde::{Deserialize, Serialize};

/// Specification for creating a new database user
///
/// # Examples
///
/// ```
/// use zqlz_admin::UserSpec;
///
/// let spec = UserSpec::new("app_user")
///     .with_password("secure_password")
///     .with_login(true);
/// assert_eq!(spec.name(), "app_user");
/// assert!(spec.can_login());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSpec {
    name: String,
    password: Option<String>,
    password_hashed: bool,
    can_login: bool,
    superuser: bool,
    create_db: bool,
    create_role: bool,
    inherit: bool,
    replication: bool,
    connection_limit: Option<i32>,
    valid_until: Option<String>,
    in_roles: Vec<String>,
    comment: Option<String>,
}

impl UserSpec {
    /// Create a new user specification
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            password: None,
            password_hashed: false,
            can_login: true,
            superuser: false,
            create_db: false,
            create_role: false,
            inherit: true,
            replication: false,
            connection_limit: None,
            valid_until: None,
            in_roles: Vec::new(),
            comment: None,
        }
    }

    /// Set the password for this user
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self.password_hashed = false;
        self
    }

    /// Set a pre-hashed password (e.g., SCRAM-SHA-256 or MD5)
    pub fn with_hashed_password(mut self, hash: impl Into<String>) -> Self {
        self.password = Some(hash.into());
        self.password_hashed = true;
        self
    }

    /// Set whether this user can log in (default: true)
    pub fn with_login(mut self, can_login: bool) -> Self {
        self.can_login = can_login;
        self
    }

    /// Set whether this user is a superuser (default: false)
    pub fn with_superuser(mut self, superuser: bool) -> Self {
        self.superuser = superuser;
        self
    }

    /// Set whether this user can create databases (default: false)
    pub fn with_create_db(mut self, create_db: bool) -> Self {
        self.create_db = create_db;
        self
    }

    /// Set whether this user can create roles (default: false)
    pub fn with_create_role(mut self, create_role: bool) -> Self {
        self.create_role = create_role;
        self
    }

    /// Set whether this user inherits privileges from roles (default: true)
    pub fn with_inherit(mut self, inherit: bool) -> Self {
        self.inherit = inherit;
        self
    }

    /// Set whether this user can initiate streaming replication (default: false)
    pub fn with_replication(mut self, replication: bool) -> Self {
        self.replication = replication;
        self
    }

    /// Set the connection limit for this user (-1 for unlimited)
    pub fn with_connection_limit(mut self, limit: i32) -> Self {
        self.connection_limit = Some(limit);
        self
    }

    /// Set when the password expires (timestamp format)
    pub fn with_valid_until(mut self, timestamp: impl Into<String>) -> Self {
        self.valid_until = Some(timestamp.into());
        self
    }

    /// Add role memberships for this user
    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.in_roles = roles;
        self
    }

    /// Add a single role membership
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.in_roles.push(role.into());
        self
    }

    /// Set a comment for this user
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Get the user name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the password (if set)
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// Check if the password is pre-hashed
    pub fn is_password_hashed(&self) -> bool {
        self.password_hashed
    }

    /// Check if this user can log in
    pub fn can_login(&self) -> bool {
        self.can_login
    }

    /// Check if this user is a superuser
    pub fn is_superuser(&self) -> bool {
        self.superuser
    }

    /// Check if this user can create databases
    pub fn can_create_db(&self) -> bool {
        self.create_db
    }

    /// Check if this user can create roles
    pub fn can_create_role(&self) -> bool {
        self.create_role
    }

    /// Check if this user inherits privileges
    pub fn inherits(&self) -> bool {
        self.inherit
    }

    /// Check if this user can initiate replication
    pub fn can_replicate(&self) -> bool {
        self.replication
    }

    /// Get the connection limit (if set)
    pub fn connection_limit(&self) -> Option<i32> {
        self.connection_limit
    }

    /// Get the password expiration timestamp (if set)
    pub fn valid_until(&self) -> Option<&str> {
        self.valid_until.as_deref()
    }

    /// Get the role memberships
    pub fn roles(&self) -> &[String] {
        &self.in_roles
    }

    /// Get the comment (if set)
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

/// Database dialect for user management
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserDialect {
    /// PostgreSQL syntax (CREATE ROLE with LOGIN)
    PostgreSQL,
    /// MySQL/MariaDB syntax (CREATE USER)
    MySQL,
    /// Microsoft SQL Server syntax (CREATE LOGIN / CREATE USER)
    MsSql,
}

impl UserDialect {
    /// Check if this dialect supports superuser privilege
    pub fn supports_superuser(&self) -> bool {
        matches!(self, UserDialect::PostgreSQL)
    }

    /// Check if this dialect supports role-based CREATE ROLE syntax
    pub fn supports_create_role(&self) -> bool {
        matches!(self, UserDialect::PostgreSQL)
    }

    /// Check if this dialect supports connection limits
    pub fn supports_connection_limit(&self) -> bool {
        matches!(self, UserDialect::PostgreSQL | UserDialect::MySQL)
    }

    /// Check if this dialect supports password expiration
    pub fn supports_password_expiration(&self) -> bool {
        matches!(self, UserDialect::PostgreSQL | UserDialect::MySQL)
    }

    /// Check if this dialect supports role inheritance
    pub fn supports_inheritance(&self) -> bool {
        matches!(self, UserDialect::PostgreSQL)
    }

    /// Check if this dialect supports replication privilege
    pub fn supports_replication(&self) -> bool {
        matches!(self, UserDialect::PostgreSQL)
    }

    /// Check if this dialect separates LOGIN and USER concepts
    pub fn separates_login_and_user(&self) -> bool {
        matches!(self, UserDialect::MsSql)
    }
}

/// Error type for user management operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserError {
    /// User name is empty
    EmptyName,
    /// Password is required but not provided
    PasswordRequired,
    /// Invalid password format
    InvalidPassword(String),
    /// Feature not supported by this dialect
    NotSupported(String),
    /// Invalid connection limit
    InvalidConnectionLimit,
    /// Invalid timestamp format
    InvalidTimestamp(String),
}

impl std::fmt::Display for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserError::EmptyName => write!(f, "User name cannot be empty"),
            UserError::PasswordRequired => write!(f, "Password is required"),
            UserError::InvalidPassword(msg) => write!(f, "Invalid password: {}", msg),
            UserError::NotSupported(feature) => {
                write!(f, "{} is not supported by this dialect", feature)
            }
            UserError::InvalidConnectionLimit => {
                write!(f, "Connection limit must be -1 or positive")
            }
            UserError::InvalidTimestamp(msg) => write!(f, "Invalid timestamp: {}", msg),
        }
    }
}

impl std::error::Error for UserError {}

/// User management service for generating user DDL statements
///
/// # Examples
///
/// ```
/// use zqlz_admin::{UserManagementService, UserDialect, UserSpec};
///
/// let service = UserManagementService::new(UserDialect::PostgreSQL);
/// let spec = UserSpec::new("app_user").with_password("secret123");
/// let sql = service.build_create_user(&spec).unwrap();
/// assert!(sql.contains("CREATE ROLE"));
/// assert!(sql.contains("app_user"));
/// ```
pub struct UserManagementService {
    dialect: UserDialect,
}

impl UserManagementService {
    /// Create a new user management service for the specified dialect
    pub fn new(dialect: UserDialect) -> Self {
        Self { dialect }
    }

    /// Get the dialect for this service
    pub fn dialect(&self) -> UserDialect {
        self.dialect
    }

    /// Validate a user specification
    pub fn validate(&self, spec: &UserSpec) -> Result<(), UserError> {
        if spec.name.trim().is_empty() {
            return Err(UserError::EmptyName);
        }

        if spec.superuser && !self.dialect.supports_superuser() {
            return Err(UserError::NotSupported("SUPERUSER".to_string()));
        }

        if spec.replication && !self.dialect.supports_replication() {
            return Err(UserError::NotSupported("REPLICATION".to_string()));
        }

        if let Some(limit) = spec.connection_limit {
            if limit < -1 {
                return Err(UserError::InvalidConnectionLimit);
            }
        }

        Ok(())
    }

    /// Build a CREATE USER/ROLE statement
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_admin::{UserManagementService, UserDialect, UserSpec};
    ///
    /// let service = UserManagementService::new(UserDialect::PostgreSQL);
    ///
    /// // Simple user with password
    /// let spec = UserSpec::new("app_user").with_password("secure123");
    /// let sql = service.build_create_user(&spec).unwrap();
    /// assert!(sql.contains("CREATE ROLE app_user"));
    /// assert!(sql.contains("LOGIN"));
    /// assert!(sql.contains("PASSWORD"));
    /// ```
    pub fn build_create_user(&self, spec: &UserSpec) -> Result<String, UserError> {
        self.validate(spec)?;

        match self.dialect {
            UserDialect::PostgreSQL => self.build_postgres_create_user(spec),
            UserDialect::MySQL => self.build_mysql_create_user(spec),
            UserDialect::MsSql => self.build_mssql_create_user(spec),
        }
    }

    /// Build a DROP USER/ROLE statement
    ///
    /// # Arguments
    /// * `name` - User name to drop
    /// * `if_exists` - Add IF EXISTS clause
    pub fn build_drop_user(&self, name: &str, if_exists: bool) -> String {
        let quoted_name = self.quote_identifier(name);
        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };

        match self.dialect {
            UserDialect::PostgreSQL => {
                format!("DROP ROLE {}{}", if_exists_clause, quoted_name)
            }
            UserDialect::MySQL => {
                format!("DROP USER {}{}", if_exists_clause, quoted_name)
            }
            UserDialect::MsSql => {
                if if_exists {
                    format!(
                        "IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '{}')\n    DROP LOGIN {}",
                        name.replace('\'', "''"),
                        quoted_name
                    )
                } else {
                    format!("DROP LOGIN {}", quoted_name)
                }
            }
        }
    }

    /// Build an ALTER USER statement to change password
    ///
    /// # Arguments
    /// * `name` - User name
    /// * `new_password` - New password
    /// * `hashed` - Whether the password is pre-hashed
    pub fn build_alter_password(&self, name: &str, new_password: &str, hashed: bool) -> String {
        let quoted_name = self.quote_identifier(name);
        let password_clause = if hashed {
            new_password.to_string()
        } else {
            format!("'{}'", new_password.replace('\'', "''"))
        };

        match self.dialect {
            UserDialect::PostgreSQL => {
                format!(
                    "ALTER ROLE {} WITH PASSWORD {}",
                    quoted_name, password_clause
                )
            }
            UserDialect::MySQL => {
                format!(
                    "ALTER USER {} IDENTIFIED BY {}",
                    quoted_name, password_clause
                )
            }
            UserDialect::MsSql => {
                format!(
                    "ALTER LOGIN {} WITH PASSWORD = {}",
                    quoted_name, password_clause
                )
            }
        }
    }

    /// Build an ALTER USER statement to rename a user
    ///
    /// # Arguments
    /// * `old_name` - Current user name
    /// * `new_name` - New user name
    pub fn build_rename_user(&self, old_name: &str, new_name: &str) -> String {
        let quoted_old = self.quote_identifier(old_name);
        let quoted_new = self.quote_identifier(new_name);

        match self.dialect {
            UserDialect::PostgreSQL => {
                format!("ALTER ROLE {} RENAME TO {}", quoted_old, quoted_new)
            }
            UserDialect::MySQL => {
                format!("RENAME USER {} TO {}", quoted_old, quoted_new)
            }
            UserDialect::MsSql => {
                format!("ALTER LOGIN {} WITH NAME = {}", quoted_old, quoted_new)
            }
        }
    }

    /// Build a GRANT ROLE statement (add user to role)
    ///
    /// # Arguments
    /// * `user_name` - User to add to role
    /// * `role_name` - Role to grant
    pub fn build_grant_role(&self, user_name: &str, role_name: &str) -> String {
        let quoted_user = self.quote_identifier(user_name);
        let quoted_role = self.quote_identifier(role_name);

        match self.dialect {
            UserDialect::PostgreSQL => {
                format!("GRANT {} TO {}", quoted_role, quoted_user)
            }
            UserDialect::MySQL => {
                format!("GRANT {} TO {}", quoted_role, quoted_user)
            }
            UserDialect::MsSql => {
                format!("ALTER ROLE {} ADD MEMBER {}", quoted_role, quoted_user)
            }
        }
    }

    /// Build a REVOKE ROLE statement (remove user from role)
    ///
    /// # Arguments
    /// * `user_name` - User to remove from role
    /// * `role_name` - Role to revoke
    pub fn build_revoke_role(&self, user_name: &str, role_name: &str) -> String {
        let quoted_user = self.quote_identifier(user_name);
        let quoted_role = self.quote_identifier(role_name);

        match self.dialect {
            UserDialect::PostgreSQL => {
                format!("REVOKE {} FROM {}", quoted_role, quoted_user)
            }
            UserDialect::MySQL => {
                format!("REVOKE {} FROM {}", quoted_role, quoted_user)
            }
            UserDialect::MsSql => {
                format!("ALTER ROLE {} DROP MEMBER {}", quoted_role, quoted_user)
            }
        }
    }

    /// Build a query to list all users/roles
    pub fn build_list_users_query(&self) -> String {
        match self.dialect {
            UserDialect::PostgreSQL => {
                "SELECT rolname AS name, rolcanlogin AS can_login, rolsuper AS is_superuser, \
                 rolcreatedb AS can_create_db, rolcreaterole AS can_create_role, \
                 rolconnlimit AS connection_limit, rolvaliduntil AS valid_until \
                 FROM pg_catalog.pg_roles \
                 WHERE rolname NOT LIKE 'pg_%' \
                 ORDER BY rolname"
                    .to_string()
            }
            UserDialect::MySQL => "SELECT User AS name, Host AS host, \
                 IF(account_locked = 'N', TRUE, FALSE) AS can_login, \
                 IF(Super_priv = 'Y', TRUE, FALSE) AS is_superuser, \
                 IF(Create_priv = 'Y', TRUE, FALSE) AS can_create_db, \
                 max_connections AS connection_limit, \
                 password_expired AS password_expired \
                 FROM mysql.user \
                 ORDER BY User"
                .to_string(),
            UserDialect::MsSql => "SELECT name, type_desc AS type, \
                 is_disabled, create_date, modify_date, \
                 default_database_name AS default_database \
                 FROM sys.server_principals \
                 WHERE type IN ('S', 'U', 'G') \
                 AND name NOT LIKE '##%' \
                 AND name NOT LIKE 'NT %' \
                 ORDER BY name"
                .to_string(),
        }
    }

    /// Build a COMMENT ON statement (PostgreSQL only)
    pub fn build_comment(&self, name: &str, comment: Option<&str>) -> Option<String> {
        if !matches!(self.dialect, UserDialect::PostgreSQL) {
            return None;
        }

        let quoted_name = self.quote_identifier(name);
        let comment_value = match comment {
            Some(c) => format!("'{}'", c.replace('\'', "''")),
            None => "NULL".to_string(),
        };

        Some(format!(
            "COMMENT ON ROLE {} IS {}",
            quoted_name, comment_value
        ))
    }

    /// Build ALTER statements to enable/disable login
    pub fn build_alter_login(&self, name: &str, enable: bool) -> String {
        let quoted_name = self.quote_identifier(name);

        match self.dialect {
            UserDialect::PostgreSQL => {
                let login_option = if enable { "LOGIN" } else { "NOLOGIN" };
                format!("ALTER ROLE {} WITH {}", quoted_name, login_option)
            }
            UserDialect::MySQL => {
                let lock_option = if enable { "UNLOCK" } else { "LOCK" };
                format!("ALTER USER {} ACCOUNT {}", quoted_name, lock_option)
            }
            UserDialect::MsSql => {
                let disable_option = if enable { "ENABLE" } else { "DISABLE" };
                format!("ALTER LOGIN {} {}", quoted_name, disable_option)
            }
        }
    }

    fn build_postgres_create_user(&self, spec: &UserSpec) -> Result<String, UserError> {
        let quoted_name = self.quote_identifier(&spec.name);
        let mut options = Vec::new();

        if spec.can_login {
            options.push("LOGIN".to_string());
        } else {
            options.push("NOLOGIN".to_string());
        }

        if let Some(password) = &spec.password {
            if spec.password_hashed {
                options.push(format!("PASSWORD '{}'", password));
            } else {
                options.push(format!("PASSWORD '{}'", password.replace('\'', "''")));
            }
        }

        if spec.superuser {
            options.push("SUPERUSER".to_string());
        }

        if spec.create_db {
            options.push("CREATEDB".to_string());
        }

        if spec.create_role {
            options.push("CREATEROLE".to_string());
        }

        if !spec.inherit {
            options.push("NOINHERIT".to_string());
        }

        if spec.replication {
            options.push("REPLICATION".to_string());
        }

        if let Some(limit) = spec.connection_limit {
            options.push(format!("CONNECTION LIMIT {}", limit));
        }

        if let Some(valid_until) = &spec.valid_until {
            options.push(format!("VALID UNTIL '{}'", valid_until.replace('\'', "''")));
        }

        let options_clause = if options.is_empty() {
            String::new()
        } else {
            format!(" WITH {}", options.join(" "))
        };

        let mut statements = vec![format!("CREATE ROLE {}{}", quoted_name, options_clause)];

        for role in &spec.in_roles {
            let quoted_role = self.quote_identifier(role);
            statements.push(format!("GRANT {} TO {}", quoted_role, quoted_name));
        }

        Ok(statements.join(";\n"))
    }

    fn build_mysql_create_user(&self, spec: &UserSpec) -> Result<String, UserError> {
        let quoted_name = self.quote_identifier(&spec.name);
        let mut parts = vec![format!("CREATE USER {}", quoted_name)];

        if let Some(password) = &spec.password {
            if spec.password_hashed {
                parts.push(format!("IDENTIFIED BY '{}'", password));
            } else {
                parts.push(format!("IDENTIFIED BY '{}'", password.replace('\'', "''")));
            }
        }

        let mut options = Vec::new();

        if let Some(limit) = spec.connection_limit {
            if limit >= 0 {
                options.push(format!("MAX_CONNECTIONS_PER_HOUR {}", limit));
            }
        }

        if let Some(valid_until) = &spec.valid_until {
            options.push(format!(
                "PASSWORD EXPIRE INTERVAL {} DAY",
                valid_until.replace('\'', "''")
            ));
        }

        if !spec.can_login {
            options.push("ACCOUNT LOCK".to_string());
        }

        if !options.is_empty() {
            parts.push(format!("WITH {}", options.join(" ")));
        }

        let mut statements = vec![parts.join(" ")];

        for role in &spec.in_roles {
            let quoted_role = self.quote_identifier(role);
            statements.push(format!("GRANT {} TO {}", quoted_role, quoted_name));
        }

        Ok(statements.join(";\n"))
    }

    fn build_mssql_create_user(&self, spec: &UserSpec) -> Result<String, UserError> {
        let quoted_name = self.quote_identifier(&spec.name);
        let mut parts = vec![format!("CREATE LOGIN {}", quoted_name)];

        if let Some(password) = &spec.password {
            if spec.password_hashed {
                parts.push(format!("WITH PASSWORD = '{}' HASHED", password));
            } else {
                parts.push(format!(
                    "WITH PASSWORD = '{}'",
                    password.replace('\'', "''")
                ));
            }
        } else {
            return Err(UserError::PasswordRequired);
        }

        let mut options = Vec::new();

        if spec.valid_until.is_some() {
            options.push("CHECK_EXPIRATION = ON, CHECK_POLICY = ON".to_string());
        }

        if !spec.can_login {
            options.push("CHECK_POLICY = OFF".to_string());
        }

        if !options.is_empty() {
            parts.push(format!(", {}", options.join(", ")));
        }

        let mut statements = vec![parts.join("")];

        if !spec.can_login {
            statements.push(format!("ALTER LOGIN {} DISABLE", quoted_name));
        }

        for role in &spec.in_roles {
            let quoted_role = self.quote_identifier(role);
            statements.push(format!(
                "ALTER SERVER ROLE {} ADD MEMBER {}",
                quoted_role, quoted_name
            ));
        }

        Ok(statements.join(";\n"))
    }

    /// Quote an identifier based on the dialect
    fn quote_identifier(&self, name: &str) -> String {
        match self.dialect {
            UserDialect::PostgreSQL => {
                if Self::needs_quoting(name) {
                    format!("\"{}\"", name.replace('"', "\"\""))
                } else {
                    name.to_string()
                }
            }
            UserDialect::MySQL => {
                if Self::needs_quoting(name) {
                    format!("`{}`", name.replace('`', "``"))
                } else {
                    name.to_string()
                }
            }
            UserDialect::MsSql => {
                if Self::needs_quoting(name) {
                    format!("[{}]", name.replace(']', "]]"))
                } else {
                    name.to_string()
                }
            }
        }
    }

    fn needs_quoting(name: &str) -> bool {
        let Some(first) = name.chars().next() else {
            return true;
        };
        if !first.is_ascii_alphabetic() && first != '_' {
            return true;
        }
        name.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_')
            || RESERVED_KEYWORDS.contains(&name.to_uppercase().as_str())
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
    "LOGIN",
    "PASSWORD",
    "DATABASE",
    "SCHEMA",
];
