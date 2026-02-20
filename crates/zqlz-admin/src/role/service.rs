//! Role management service implementation
//!
//! Provides functionality for creating, altering, and dropping database roles
//! and managing permissions across different database dialects.
//!
//! Unlike users, roles primarily represent groups of permissions that can be
//! granted to users or other roles.

use serde::{Deserialize, Serialize};

/// Specification for creating a new database role
///
/// # Examples
///
/// ```
/// use zqlz_admin::RoleSpec;
///
/// let spec = RoleSpec::new("read_only")
///     .with_comment("Read-only access role");
/// assert_eq!(spec.name(), "read_only");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleSpec {
    name: String,
    inherit: bool,
    member_of: Vec<String>,
    members: Vec<String>,
    admin_members: Vec<String>,
    comment: Option<String>,
}

impl RoleSpec {
    /// Create a new role specification
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            inherit: true,
            member_of: Vec::new(),
            members: Vec::new(),
            admin_members: Vec::new(),
            comment: None,
        }
    }

    /// Set whether role members inherit privileges (default: true)
    pub fn with_inherit(mut self, inherit: bool) -> Self {
        self.inherit = inherit;
        self
    }

    /// Add roles that this role is a member of
    pub fn with_member_of(mut self, roles: Vec<String>) -> Self {
        self.member_of = roles;
        self
    }

    /// Add a single role that this role is a member of
    pub fn in_role(mut self, role: impl Into<String>) -> Self {
        self.member_of.push(role.into());
        self
    }

    /// Add members (users/roles) that belong to this role
    pub fn with_members(mut self, members: Vec<String>) -> Self {
        self.members = members;
        self
    }

    /// Add a single member to this role
    pub fn with_member(mut self, member: impl Into<String>) -> Self {
        self.members.push(member.into());
        self
    }

    /// Add admin members (can grant this role to others)
    pub fn with_admin_members(mut self, admins: Vec<String>) -> Self {
        self.admin_members = admins;
        self
    }

    /// Add a single admin member
    pub fn with_admin_member(mut self, admin: impl Into<String>) -> Self {
        self.admin_members.push(admin.into());
        self
    }

    /// Set a comment for this role
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Get the role name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if members inherit privileges
    pub fn inherits(&self) -> bool {
        self.inherit
    }

    /// Get roles this role is a member of
    pub fn member_of(&self) -> &[String] {
        &self.member_of
    }

    /// Get members of this role
    pub fn members(&self) -> &[String] {
        &self.members
    }

    /// Get admin members of this role
    pub fn admin_members(&self) -> &[String] {
        &self.admin_members
    }

    /// Get the comment (if set)
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

/// Privilege type for object grants
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Privilege {
    /// SELECT privilege
    Select,
    /// INSERT privilege
    Insert,
    /// UPDATE privilege
    Update,
    /// DELETE privilege
    Delete,
    /// TRUNCATE privilege
    Truncate,
    /// REFERENCES privilege
    References,
    /// TRIGGER privilege
    Trigger,
    /// EXECUTE privilege (for functions/procedures)
    Execute,
    /// USAGE privilege (for schemas, sequences)
    Usage,
    /// CREATE privilege (for schemas, databases)
    Create,
    /// ALL privileges
    All,
}

impl Privilege {
    /// Convert privilege to SQL keyword
    pub fn as_sql(&self) -> &'static str {
        match self {
            Privilege::Select => "SELECT",
            Privilege::Insert => "INSERT",
            Privilege::Update => "UPDATE",
            Privilege::Delete => "DELETE",
            Privilege::Truncate => "TRUNCATE",
            Privilege::References => "REFERENCES",
            Privilege::Trigger => "TRIGGER",
            Privilege::Execute => "EXECUTE",
            Privilege::Usage => "USAGE",
            Privilege::Create => "CREATE",
            Privilege::All => "ALL PRIVILEGES",
        }
    }
}

/// Object type for privilege grants
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectType {
    /// Table privilege
    Table(String),
    /// All tables in schema
    AllTablesInSchema(String),
    /// View privilege
    View(String),
    /// Sequence privilege
    Sequence(String),
    /// All sequences in schema
    AllSequencesInSchema(String),
    /// Function privilege
    Function(String),
    /// All functions in schema
    AllFunctionsInSchema(String),
    /// Procedure privilege
    Procedure(String),
    /// Schema privilege
    Schema(String),
    /// Database privilege
    Database(String),
}

impl ObjectType {
    /// Get the object type keyword for SQL
    pub fn type_keyword(&self) -> &'static str {
        match self {
            ObjectType::Table(_) => "TABLE",
            ObjectType::AllTablesInSchema(_) => "ALL TABLES IN SCHEMA",
            ObjectType::View(_) => "TABLE", // Views are treated as tables in most databases
            ObjectType::Sequence(_) => "SEQUENCE",
            ObjectType::AllSequencesInSchema(_) => "ALL SEQUENCES IN SCHEMA",
            ObjectType::Function(_) => "FUNCTION",
            ObjectType::AllFunctionsInSchema(_) => "ALL FUNCTIONS IN SCHEMA",
            ObjectType::Procedure(_) => "PROCEDURE",
            ObjectType::Schema(_) => "SCHEMA",
            ObjectType::Database(_) => "DATABASE",
        }
    }

    /// Get the object name
    pub fn name(&self) -> &str {
        match self {
            ObjectType::Table(name)
            | ObjectType::AllTablesInSchema(name)
            | ObjectType::View(name)
            | ObjectType::Sequence(name)
            | ObjectType::AllSequencesInSchema(name)
            | ObjectType::Function(name)
            | ObjectType::AllFunctionsInSchema(name)
            | ObjectType::Procedure(name)
            | ObjectType::Schema(name)
            | ObjectType::Database(name) => name,
        }
    }
}

/// Database dialect for role management
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleDialect {
    /// PostgreSQL syntax
    PostgreSQL,
    /// MySQL/MariaDB syntax
    MySQL,
    /// Microsoft SQL Server syntax
    MsSql,
}

impl RoleDialect {
    /// Check if this dialect supports role inheritance
    pub fn supports_inherit(&self) -> bool {
        matches!(self, RoleDialect::PostgreSQL)
    }

    /// Check if this dialect supports WITH ADMIN OPTION
    pub fn supports_admin_option(&self) -> bool {
        matches!(self, RoleDialect::PostgreSQL | RoleDialect::MySQL)
    }

    /// Check if this dialect supports schema-level grants
    pub fn supports_schema_grants(&self) -> bool {
        matches!(self, RoleDialect::PostgreSQL | RoleDialect::MsSql)
    }

    /// Check if this dialect supports GRANT OPTION
    pub fn supports_grant_option(&self) -> bool {
        true
    }

    /// Check if this dialect supports ALL TABLES IN SCHEMA syntax
    pub fn supports_all_in_schema(&self) -> bool {
        matches!(self, RoleDialect::PostgreSQL)
    }
}

/// Error type for role management operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleError {
    /// Role name is empty
    EmptyName,
    /// Object name is empty
    EmptyObjectName,
    /// No privileges specified
    NoPrivileges,
    /// Feature not supported by this dialect
    NotSupported(String),
    /// Invalid privilege for object type
    InvalidPrivilege(String),
}

impl std::fmt::Display for RoleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoleError::EmptyName => write!(f, "Role name cannot be empty"),
            RoleError::EmptyObjectName => write!(f, "Object name cannot be empty"),
            RoleError::NoPrivileges => write!(f, "At least one privilege is required"),
            RoleError::NotSupported(feature) => {
                write!(f, "{} is not supported by this dialect", feature)
            }
            RoleError::InvalidPrivilege(msg) => write!(f, "Invalid privilege: {}", msg),
        }
    }
}

impl std::error::Error for RoleError {}

/// Role management service for generating role DDL and grant statements
///
/// # Examples
///
/// ```
/// use zqlz_admin::{RoleManagementService, RoleDialect, RoleSpec};
///
/// let service = RoleManagementService::new(RoleDialect::PostgreSQL);
/// let spec = RoleSpec::new("read_only");
/// let sql = service.build_create_role(&spec).unwrap();
/// assert!(sql.contains("CREATE ROLE"));
/// ```
pub struct RoleManagementService {
    dialect: RoleDialect,
}

impl RoleManagementService {
    /// Create a new role management service for the specified dialect
    pub fn new(dialect: RoleDialect) -> Self {
        Self { dialect }
    }

    /// Get the dialect for this service
    pub fn dialect(&self) -> RoleDialect {
        self.dialect
    }

    /// Validate a role specification
    pub fn validate(&self, spec: &RoleSpec) -> Result<(), RoleError> {
        if spec.name.trim().is_empty() {
            return Err(RoleError::EmptyName);
        }

        if !spec.inherit && !self.dialect.supports_inherit() {
            return Err(RoleError::NotSupported("NOINHERIT".to_string()));
        }

        if !spec.admin_members.is_empty() && !self.dialect.supports_admin_option() {
            return Err(RoleError::NotSupported("WITH ADMIN OPTION".to_string()));
        }

        Ok(())
    }

    /// Build a CREATE ROLE statement
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_admin::{RoleManagementService, RoleDialect, RoleSpec};
    ///
    /// let service = RoleManagementService::new(RoleDialect::PostgreSQL);
    /// let spec = RoleSpec::new("read_only").with_comment("Read-only access");
    /// let sql = service.build_create_role(&spec).unwrap();
    /// assert!(sql.contains("CREATE ROLE read_only"));
    /// ```
    pub fn build_create_role(&self, spec: &RoleSpec) -> Result<String, RoleError> {
        self.validate(spec)?;

        match self.dialect {
            RoleDialect::PostgreSQL => self.build_postgres_create_role(spec),
            RoleDialect::MySQL => self.build_mysql_create_role(spec),
            RoleDialect::MsSql => self.build_mssql_create_role(spec),
        }
    }

    /// Build a DROP ROLE statement
    ///
    /// # Arguments
    /// * `name` - Role name to drop
    /// * `if_exists` - Add IF EXISTS clause
    pub fn build_drop_role(&self, name: &str, if_exists: bool) -> String {
        let quoted_name = self.quote_identifier(name);
        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };

        match self.dialect {
            RoleDialect::PostgreSQL => {
                format!("DROP ROLE {}{}", if_exists_clause, quoted_name)
            }
            RoleDialect::MySQL => {
                format!("DROP ROLE {}{}", if_exists_clause, quoted_name)
            }
            RoleDialect::MsSql => {
                if if_exists {
                    format!(
                        "IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '{}' AND type = 'R')\n    DROP ROLE {}",
                        name.replace('\'', "''"),
                        quoted_name
                    )
                } else {
                    format!("DROP ROLE {}", quoted_name)
                }
            }
        }
    }

    /// Build a GRANT statement for object privileges
    ///
    /// # Arguments
    /// * `privileges` - Privileges to grant
    /// * `object` - Object to grant on
    /// * `role` - Role to grant to
    /// * `with_grant_option` - Whether grantee can grant to others
    pub fn build_grant_privileges(
        &self,
        privileges: &[Privilege],
        object: &ObjectType,
        role: &str,
        with_grant_option: bool,
    ) -> Result<String, RoleError> {
        if privileges.is_empty() {
            return Err(RoleError::NoPrivileges);
        }

        if object.name().trim().is_empty() {
            return Err(RoleError::EmptyObjectName);
        }

        self.validate_privileges_for_object(privileges, object)?;

        let priv_list: Vec<&str> = privileges.iter().map(|p| p.as_sql()).collect();
        let priv_str = priv_list.join(", ");

        let quoted_role = self.quote_identifier(role);
        let object_clause = self.build_object_clause(object);

        let grant_option = if with_grant_option {
            match self.dialect {
                RoleDialect::PostgreSQL => " WITH GRANT OPTION",
                RoleDialect::MySQL => " WITH GRANT OPTION",
                RoleDialect::MsSql => " WITH GRANT OPTION",
            }
        } else {
            ""
        };

        Ok(format!(
            "GRANT {} ON {} TO {}{}",
            priv_str, object_clause, quoted_role, grant_option
        ))
    }

    /// Build a REVOKE statement for object privileges
    ///
    /// # Arguments
    /// * `privileges` - Privileges to revoke
    /// * `object` - Object to revoke from
    /// * `role` - Role to revoke from
    /// * `cascade` - Whether to revoke from dependent grantees (PostgreSQL only)
    pub fn build_revoke_privileges(
        &self,
        privileges: &[Privilege],
        object: &ObjectType,
        role: &str,
        cascade: bool,
    ) -> Result<String, RoleError> {
        if privileges.is_empty() {
            return Err(RoleError::NoPrivileges);
        }

        if object.name().trim().is_empty() {
            return Err(RoleError::EmptyObjectName);
        }

        let priv_list: Vec<&str> = privileges.iter().map(|p| p.as_sql()).collect();
        let priv_str = priv_list.join(", ");

        let quoted_role = self.quote_identifier(role);
        let object_clause = self.build_object_clause(object);

        let cascade_clause = if cascade && matches!(self.dialect, RoleDialect::PostgreSQL) {
            " CASCADE"
        } else {
            ""
        };

        Ok(format!(
            "REVOKE {} ON {} FROM {}{}",
            priv_str, object_clause, quoted_role, cascade_clause
        ))
    }

    /// Build a GRANT role membership statement
    ///
    /// # Arguments
    /// * `role_name` - Role to grant
    /// * `grantee` - User/role to grant to
    /// * `with_admin_option` - Whether grantee can grant this role to others
    pub fn build_grant_role(
        &self,
        role_name: &str,
        grantee: &str,
        with_admin_option: bool,
    ) -> String {
        let quoted_role = self.quote_identifier(role_name);
        let quoted_grantee = self.quote_identifier(grantee);

        let admin_option = if with_admin_option {
            match self.dialect {
                RoleDialect::PostgreSQL => " WITH ADMIN OPTION",
                RoleDialect::MySQL => " WITH ADMIN OPTION",
                RoleDialect::MsSql => "", // SQL Server handles this differently
            }
        } else {
            ""
        };

        match self.dialect {
            RoleDialect::PostgreSQL => {
                format!(
                    "GRANT {} TO {}{}",
                    quoted_role, quoted_grantee, admin_option
                )
            }
            RoleDialect::MySQL => {
                format!(
                    "GRANT {} TO {}{}",
                    quoted_role, quoted_grantee, admin_option
                )
            }
            RoleDialect::MsSql => {
                format!("ALTER ROLE {} ADD MEMBER {}", quoted_role, quoted_grantee)
            }
        }
    }

    /// Build a REVOKE role membership statement
    ///
    /// # Arguments
    /// * `role_name` - Role to revoke
    /// * `grantee` - User/role to revoke from
    pub fn build_revoke_role(&self, role_name: &str, grantee: &str) -> String {
        let quoted_role = self.quote_identifier(role_name);
        let quoted_grantee = self.quote_identifier(grantee);

        match self.dialect {
            RoleDialect::PostgreSQL => {
                format!("REVOKE {} FROM {}", quoted_role, quoted_grantee)
            }
            RoleDialect::MySQL => {
                format!("REVOKE {} FROM {}", quoted_role, quoted_grantee)
            }
            RoleDialect::MsSql => {
                format!("ALTER ROLE {} DROP MEMBER {}", quoted_role, quoted_grantee)
            }
        }
    }

    /// Build a GRANT statement for default privileges (PostgreSQL only)
    ///
    /// # Arguments
    /// * `privileges` - Privileges to grant
    /// * `object_type` - Type of objects (tables, sequences, functions)
    /// * `schema` - Schema where defaults apply
    /// * `role` - Role to grant to
    pub fn build_alter_default_privileges(
        &self,
        privileges: &[Privilege],
        object_type: &str,
        schema: &str,
        role: &str,
    ) -> Result<Option<String>, RoleError> {
        if !matches!(self.dialect, RoleDialect::PostgreSQL) {
            return Ok(None);
        }

        if privileges.is_empty() {
            return Err(RoleError::NoPrivileges);
        }

        let priv_list: Vec<&str> = privileges.iter().map(|p| p.as_sql()).collect();
        let priv_str = priv_list.join(", ");

        let quoted_schema = self.quote_identifier(schema);
        let quoted_role = self.quote_identifier(role);

        Ok(Some(format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT {} ON {} TO {}",
            quoted_schema,
            priv_str,
            object_type.to_uppercase(),
            quoted_role
        )))
    }

    /// Build a query to list all roles
    pub fn build_list_roles_query(&self) -> String {
        match self.dialect {
            RoleDialect::PostgreSQL => "SELECT rolname AS name, rolcanlogin AS can_login, \
                 rolcreatedb AS can_create_db, rolcreaterole AS can_create_role, \
                 rolinherit AS inherits, rolsuper AS is_superuser \
                 FROM pg_catalog.pg_roles \
                 WHERE rolname NOT LIKE 'pg_%' \
                 ORDER BY rolname"
                .to_string(),
            RoleDialect::MySQL => "SELECT User AS name, Host AS host, \
                 IF(account_locked = 'N', TRUE, FALSE) AS is_locked \
                 FROM mysql.user \
                 WHERE authentication_string = '' \
                 ORDER BY User"
                .to_string(),
            RoleDialect::MsSql => "SELECT name, type_desc AS type, \
                 create_date, modify_date \
                 FROM sys.database_principals \
                 WHERE type = 'R' \
                 AND name NOT IN ('public', 'db_owner', 'db_accessadmin', \
                     'db_securityadmin', 'db_ddladmin', 'db_backupoperator', \
                     'db_datareader', 'db_datawriter', 'db_denydatareader', \
                     'db_denydatawriter') \
                 ORDER BY name"
                .to_string(),
        }
    }

    /// Build a query to list role members
    pub fn build_list_role_members_query(&self, role_name: &str) -> String {
        match self.dialect {
            RoleDialect::PostgreSQL => {
                format!(
                    "SELECT m.rolname AS member_name, r.rolname AS role_name, \
                     am.admin_option \
                     FROM pg_catalog.pg_roles r \
                     JOIN pg_catalog.pg_auth_members am ON r.oid = am.roleid \
                     JOIN pg_catalog.pg_roles m ON am.member = m.oid \
                     WHERE r.rolname = '{}' \
                     ORDER BY m.rolname",
                    role_name.replace('\'', "''")
                )
            }
            RoleDialect::MySQL => {
                format!(
                    "SELECT FROM_USER AS member_name, TO_USER AS role_name, \
                     WITH_ADMIN_OPTION AS admin_option \
                     FROM mysql.role_edges \
                     WHERE TO_USER = '{}' \
                     ORDER BY FROM_USER",
                    role_name.replace('\'', "''")
                )
            }
            RoleDialect::MsSql => {
                format!(
                    "SELECT m.name AS member_name, r.name AS role_name \
                     FROM sys.database_role_members rm \
                     JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id \
                     JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id \
                     WHERE r.name = '{}' \
                     ORDER BY m.name",
                    role_name.replace('\'', "''")
                )
            }
        }
    }

    /// Build a COMMENT ON statement (PostgreSQL only)
    pub fn build_comment(&self, name: &str, comment: Option<&str>) -> Option<String> {
        if !matches!(self.dialect, RoleDialect::PostgreSQL) {
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

    /// Build ALTER ROLE to rename a role
    pub fn build_rename_role(&self, old_name: &str, new_name: &str) -> String {
        let quoted_old = self.quote_identifier(old_name);
        let quoted_new = self.quote_identifier(new_name);

        match self.dialect {
            RoleDialect::PostgreSQL => {
                format!("ALTER ROLE {} RENAME TO {}", quoted_old, quoted_new)
            }
            RoleDialect::MySQL => {
                // MySQL doesn't have direct rename - need to create new and transfer
                format!(
                    "-- MySQL doesn't support RENAME ROLE directly\n\
                     -- Create new role and transfer members manually"
                )
            }
            RoleDialect::MsSql => {
                format!("ALTER ROLE {} WITH NAME = {}", quoted_old, quoted_new)
            }
        }
    }

    fn build_postgres_create_role(&self, spec: &RoleSpec) -> Result<String, RoleError> {
        let quoted_name = self.quote_identifier(&spec.name);
        let mut options = Vec::new();

        options.push("NOLOGIN".to_string());

        if !spec.inherit {
            options.push("NOINHERIT".to_string());
        }

        let options_clause = if options.is_empty() {
            String::new()
        } else {
            format!(" WITH {}", options.join(" "))
        };

        let mut statements = vec![format!("CREATE ROLE {}{}", quoted_name, options_clause)];

        // Add role memberships
        for role in &spec.member_of {
            let quoted_role = self.quote_identifier(role);
            statements.push(format!("GRANT {} TO {}", quoted_role, quoted_name));
        }

        // Add members
        for member in &spec.members {
            let quoted_member = self.quote_identifier(member);
            statements.push(format!("GRANT {} TO {}", quoted_name, quoted_member));
        }

        // Add admin members
        for admin in &spec.admin_members {
            let quoted_admin = self.quote_identifier(admin);
            statements.push(format!(
                "GRANT {} TO {} WITH ADMIN OPTION",
                quoted_name, quoted_admin
            ));
        }

        Ok(statements.join(";\n"))
    }

    fn build_mysql_create_role(&self, spec: &RoleSpec) -> Result<String, RoleError> {
        let quoted_name = self.quote_identifier(&spec.name);
        let mut statements = vec![format!("CREATE ROLE {}", quoted_name)];

        // Add role memberships
        for role in &spec.member_of {
            let quoted_role = self.quote_identifier(role);
            statements.push(format!("GRANT {} TO {}", quoted_role, quoted_name));
        }

        // Add members
        for member in &spec.members {
            let quoted_member = self.quote_identifier(member);
            statements.push(format!("GRANT {} TO {}", quoted_name, quoted_member));
        }

        // Add admin members
        for admin in &spec.admin_members {
            let quoted_admin = self.quote_identifier(admin);
            statements.push(format!(
                "GRANT {} TO {} WITH ADMIN OPTION",
                quoted_name, quoted_admin
            ));
        }

        Ok(statements.join(";\n"))
    }

    fn build_mssql_create_role(&self, spec: &RoleSpec) -> Result<String, RoleError> {
        let quoted_name = self.quote_identifier(&spec.name);
        let mut statements = vec![format!("CREATE ROLE {}", quoted_name)];

        // Add members (SQL Server only supports adding members to roles, not roles to roles easily)
        for member in &spec.members {
            let quoted_member = self.quote_identifier(member);
            statements.push(format!(
                "ALTER ROLE {} ADD MEMBER {}",
                quoted_name, quoted_member
            ));
        }

        Ok(statements.join(";\n"))
    }

    fn build_object_clause(&self, object: &ObjectType) -> String {
        let quoted_name = self.quote_identifier(object.name());

        match object {
            ObjectType::Table(_) => format!("TABLE {}", quoted_name),
            ObjectType::AllTablesInSchema(_) => {
                format!("ALL TABLES IN SCHEMA {}", quoted_name)
            }
            ObjectType::View(_) => format!("TABLE {}", quoted_name), // Views treated as tables
            ObjectType::Sequence(_) => format!("SEQUENCE {}", quoted_name),
            ObjectType::AllSequencesInSchema(_) => {
                format!("ALL SEQUENCES IN SCHEMA {}", quoted_name)
            }
            ObjectType::Function(_) => format!("FUNCTION {}", quoted_name),
            ObjectType::AllFunctionsInSchema(_) => {
                format!("ALL FUNCTIONS IN SCHEMA {}", quoted_name)
            }
            ObjectType::Procedure(_) => format!("PROCEDURE {}", quoted_name),
            ObjectType::Schema(_) => format!("SCHEMA {}", quoted_name),
            ObjectType::Database(_) => format!("DATABASE {}", quoted_name),
        }
    }

    fn validate_privileges_for_object(
        &self,
        privileges: &[Privilege],
        object: &ObjectType,
    ) -> Result<(), RoleError> {
        for privilege in privileges {
            match (privilege, object) {
                // Table/View privileges
                (
                    Privilege::Select | Privilege::Insert | Privilege::Update | Privilege::Delete,
                    ObjectType::Table(_) | ObjectType::View(_) | ObjectType::AllTablesInSchema(_),
                ) => {}

                // TRUNCATE is PostgreSQL-specific for tables
                (Privilege::Truncate, ObjectType::Table(_) | ObjectType::AllTablesInSchema(_))
                    if matches!(self.dialect, RoleDialect::PostgreSQL) => {}

                // REFERENCES and TRIGGER for tables
                (
                    Privilege::References | Privilege::Trigger,
                    ObjectType::Table(_) | ObjectType::AllTablesInSchema(_),
                ) => {}

                // EXECUTE for functions/procedures
                (
                    Privilege::Execute,
                    ObjectType::Function(_)
                    | ObjectType::Procedure(_)
                    | ObjectType::AllFunctionsInSchema(_),
                ) => {}

                // USAGE for schemas/sequences
                (
                    Privilege::Usage,
                    ObjectType::Schema(_)
                    | ObjectType::Sequence(_)
                    | ObjectType::AllSequencesInSchema(_),
                ) => {}

                // CREATE for schemas/databases
                (Privilege::Create, ObjectType::Schema(_) | ObjectType::Database(_)) => {}

                // ALL is valid for everything
                (Privilege::All, _) => {}

                _ => {
                    return Err(RoleError::InvalidPrivilege(format!(
                        "{} is not valid for {}",
                        privilege.as_sql(),
                        object.type_keyword()
                    )));
                }
            }
        }
        Ok(())
    }

    /// Quote an identifier based on the dialect
    fn quote_identifier(&self, name: &str) -> String {
        match self.dialect {
            RoleDialect::PostgreSQL => {
                if Self::needs_quoting(name) {
                    format!("\"{}\"", name.replace('"', "\"\""))
                } else {
                    name.to_string()
                }
            }
            RoleDialect::MySQL => {
                if Self::needs_quoting(name) {
                    format!("`{}`", name.replace('`', "``"))
                } else {
                    name.to_string()
                }
            }
            RoleDialect::MsSql => {
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
    "SCHEMA",
    "DATABASE",
    "PUBLIC",
];
