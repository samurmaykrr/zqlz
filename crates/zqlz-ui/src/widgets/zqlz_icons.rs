//! ZQLZ-specific icons using Phosphor icons.
//!
//! These icons supplement gpui-component's IconName with database-specific icons.
//! They implement `IconNamed` so they can be used with the `Icon` component.

use gpui::{
    App, IntoElement, Pixels, RenderOnce, SharedString, StyleRefinement, Styled, Window, img, px,
};

use crate::widgets::IconNamed;

/// Database logos rendered as PNG images to preserve original colors.
///
/// Unlike regular icons which use SVG with theme colors, database logos
/// need to display their original brand colors. PNG format is used because
/// GPUI's SVG rendering applies text_color which overrides all fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseLogo {
    SQLite,
    Turso,
    PostgreSQL,
    MySQL,
    MariaDB,
    Redis,
    MongoDB,
    ClickHouse,
    DuckDB,
    MsSql,
}

impl DatabaseLogo {
    /// Returns the PNG asset path for this logo
    pub fn path(self) -> SharedString {
        match self {
            Self::SQLite => "icons/sqlite.png",
            Self::Turso => "icons/turso.png",
            Self::PostgreSQL => "icons/postgresql.png",
            Self::MySQL => "icons/mysql.png",
            Self::MariaDB => "icons/mariadb.png",
            Self::Redis => "icons/redis.png",
            Self::MongoDB => "icons/mongodb.png",
            Self::ClickHouse => "icons/clickhouse.png",
            Self::DuckDB => "icons/duckdb.png",
            Self::MsSql => "icons/mssql.png",
        }
        .into()
    }

    /// Create a renderable element with the specified size
    pub fn with_size(self, size: Pixels) -> DatabaseLogoElement {
        DatabaseLogoElement {
            logo: self,
            size,
            style: StyleRefinement::default(),
        }
    }

    /// Create a small logo (16px)
    pub fn small(self) -> DatabaseLogoElement {
        self.with_size(px(16.0))
    }

    /// Create a medium logo (24px)
    pub fn medium(self) -> DatabaseLogoElement {
        self.with_size(px(24.0))
    }

    /// Create a large logo (40px)
    pub fn large(self) -> DatabaseLogoElement {
        self.with_size(px(40.0))
    }
}

/// A renderable database logo element
#[derive(IntoElement)]
pub struct DatabaseLogoElement {
    logo: DatabaseLogo,
    size: Pixels,
    style: StyleRefinement,
}

impl Styled for DatabaseLogoElement {
    fn style(&mut self) -> &mut StyleRefinement {
        &mut self.style
    }
}

impl RenderOnce for DatabaseLogoElement {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        img(self.logo.path()).size(self.size)
    }
}

/// Database-specific icons for ZQLZ.
///
/// These icons use the Phosphor icon set and are embedded in the zqlz-app assets.
/// They implement `IconNamed` so they can be used with gpui-component's `Icon` component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZqlzIcon {
    // Database objects
    Database,
    Table,
    Columns,
    Rows,
    Key,
    Link,
    ListBullets,
    LightningBolt,
    Function,
    Stack,

    // Database logos (SVG versions - use DatabaseLogo for colored versions)
    SQLite,
    Turso,
    PostgreSQL,
    MySQL,
    MariaDB,
    Redis,
    MongoDB,
    ClickHouse,
    DuckDB,
    MsSql,

    // Connection
    Plug,
    PlugsConnected,

    // Query execution
    Play,
    Stop,
    Terminal,
    Code,
    FileSql,

    // File operations
    FloppyDisk,
    Folder,
    FolderOpen,

    // Settings & navigation
    Gear,
    MagnifyingGlass,
    Plus,
    Minus,
    X,
    Check,

    // Carets (for tree views)
    CaretRight,
    CaretDown,
    CaretLeft,
    CaretUp,

    // Actions
    ArrowsClockwise,
    ArrowUp,
    ArrowDown,
    Export,
    Import,
    Download,
    Upload,
    Copy,
    Trash,
    Pencil,
    Ellipsis,
    Undo,
    Redo,

    // Status indicators
    Warning,
    Info,
    CheckCircle,
    XCircle,
    Lightning,
    Clock,

    // Data types
    Hash,
    TextAa,
    Calendar,
    ToggleLeft,
    BracketsCurly,
    ListNumbers,

    // View
    Eye,
    TreeStructure,
    Funnel,
    SortAscending,
    SortDescending,

    // Text formatting
    TextWrap,
    MagicWand,
    Lightbulb,
    TextIndent,
}

impl IconNamed for ZqlzIcon {
    fn path(self) -> SharedString {
        match self {
            // Database objects
            Self::Database => "icons/database.svg",
            Self::Table => "icons/table.svg",
            Self::Columns => "icons/columns.svg",
            Self::Rows => "icons/rows.svg",
            Self::Key => "icons/key.svg",
            Self::Link => "icons/link.svg",
            Self::ListBullets => "icons/list-bullets.svg",
            Self::LightningBolt => "icons/lightning-bolt.svg",
            Self::Function => "icons/function.svg",
            Self::Stack => "icons/stack.svg",

            // Database logos
            Self::SQLite => "icons/sqlite.svg",
            Self::Turso => "icons/turso.svg",
            Self::PostgreSQL => "icons/postgresql.svg",
            Self::MySQL => "icons/mysql.svg",
            Self::MariaDB => "icons/mariadb.svg",
            Self::Redis => "icons/redis.svg",
            Self::MongoDB => "icons/mongodb.svg",
            Self::ClickHouse => "icons/clickhouse.svg",
            Self::DuckDB => "icons/duckdb.svg",
            Self::MsSql => "icons/mssql.svg",

            // Connection
            Self::Plug => "icons/plug.svg",
            Self::PlugsConnected => "icons/plugs-connected.svg",

            // Query execution
            Self::Play => "icons/play.svg",
            Self::Stop => "icons/stop.svg",
            Self::Terminal => "icons/terminal.svg",
            Self::Code => "icons/code.svg",
            Self::FileSql => "icons/file-sql.svg",

            // File operations
            Self::FloppyDisk => "icons/floppy-disk.svg",
            Self::Folder => "icons/folder.svg",
            Self::FolderOpen => "icons/folder-open.svg",

            // Settings & navigation
            Self::Gear => "icons/gear.svg",
            Self::MagnifyingGlass => "icons/magnifying-glass.svg",
            Self::Plus => "icons/plus.svg",
            Self::Minus => "icons/minus.svg",
            Self::X => "icons/x.svg",
            Self::Check => "icons/check.svg",

            // Carets
            Self::CaretRight => "icons/caret-right.svg",
            Self::CaretDown => "icons/caret-down.svg",
            Self::CaretLeft => "icons/caret-left.svg",
            Self::CaretUp => "icons/caret-up.svg",

            // Actions
            Self::ArrowsClockwise => "icons/arrows-clockwise.svg",
            Self::ArrowUp => "icons/arrow-up.svg",
            Self::ArrowDown => "icons/arrow-down.svg",
            Self::Export => "icons/arrow-square-out.svg",
            Self::Import => "icons/arrow-square-in.svg",
            Self::Download => "icons/download.svg",
            Self::Upload => "icons/upload.svg",
            Self::Copy => "icons/copy.svg",
            Self::Trash => "icons/trash.svg",
            Self::Pencil => "icons/pencil.svg",
            Self::Ellipsis => "icons/ellipsis.svg",
            Self::Undo => "icons/undo.svg",
            Self::Redo => "icons/redo.svg",

            // Status indicators
            Self::Warning => "icons/warning.svg",
            Self::Info => "icons/info.svg",
            Self::CheckCircle => "icons/check-circle.svg",
            Self::XCircle => "icons/x-circle.svg",
            Self::Lightning => "icons/lightning.svg",
            Self::Clock => "icons/clock.svg",

            // Data types
            Self::Hash => "icons/hash.svg",
            Self::TextAa => "icons/text-aa.svg",
            Self::Calendar => "icons/calendar.svg",
            Self::ToggleLeft => "icons/toggle-left.svg",
            Self::BracketsCurly => "icons/brackets-curly.svg",
            Self::ListNumbers => "icons/list-numbers.svg",

            // View
            Self::Eye => "icons/eye.svg",
            Self::TreeStructure => "icons/tree-structure.svg",
            Self::Funnel => "icons/funnel.svg",
            Self::SortAscending => "icons/sort-ascending.svg",
            Self::SortDescending => "icons/sort-descending.svg",

            // Text formatting
            Self::TextWrap => "icons/text-wrap.svg",
            Self::MagicWand => "icons/magic-wand.svg",
            Self::Lightbulb => "icons/lightbulb.svg",
            Self::TextIndent => "icons/text-indent.svg",
        }
        .into()
    }
}

pub fn object_icon_from_key(icon_key: &str) -> ZqlzIcon {
    match icon_key {
        "view" | "materialized_view" => ZqlzIcon::Eye,
        "function" | "procedure" => ZqlzIcon::Function,
        "trigger" => ZqlzIcon::LightningBolt,
        "event" => ZqlzIcon::Calendar,
        "redis_database" => ZqlzIcon::Database,
        "sequence" => ZqlzIcon::ListNumbers,
        "type" => ZqlzIcon::BracketsCurly,
        "index" | "policy" | "collation" => ZqlzIcon::Key,
        "foreign_server" | "foreign_data_wrapper" => ZqlzIcon::Link,
        "event_trigger" => ZqlzIcon::Lightning,
        "language" => ZqlzIcon::Code,
        "schema" | "extension" | "publication" | "subscription" | "tablespace" => ZqlzIcon::Stack,
        _ => ZqlzIcon::Table,
    }
}

pub fn action_icon_from_key(icon_key: &str) -> Option<ZqlzIcon> {
    match icon_key {
        "refresh" => Some(ZqlzIcon::ArrowsClockwise),
        "create" | "add" | "plus" => Some(ZqlzIcon::Plus),
        "import" => Some(ZqlzIcon::Import),
        "export" => Some(ZqlzIcon::Export),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{ZqlzIcon, action_icon_from_key, object_icon_from_key};

    #[test]
    fn object_icon_from_key_resolves_known_database_object_icons() {
        assert_eq!(object_icon_from_key("view"), ZqlzIcon::Eye);
        assert_eq!(object_icon_from_key("function"), ZqlzIcon::Function);
        assert_eq!(object_icon_from_key("trigger"), ZqlzIcon::LightningBolt);
        assert_eq!(object_icon_from_key("event"), ZqlzIcon::Calendar);
        assert_eq!(object_icon_from_key("redis_database"), ZqlzIcon::Database);
        assert_eq!(object_icon_from_key("sequence"), ZqlzIcon::ListNumbers);
    }

    #[test]
    fn object_icon_from_key_degrades_unknown_keys_to_table_icon() {
        assert_eq!(object_icon_from_key("driver_custom_kind"), ZqlzIcon::Table);
    }

    #[test]
    fn action_icon_from_key_resolves_manifest_action_icons() {
        assert_eq!(
            action_icon_from_key("refresh"),
            Some(ZqlzIcon::ArrowsClockwise)
        );
        assert_eq!(action_icon_from_key("create"), Some(ZqlzIcon::Plus));
        assert_eq!(action_icon_from_key("import"), Some(ZqlzIcon::Import));
        assert_eq!(action_icon_from_key("export"), Some(ZqlzIcon::Export));
        assert_eq!(action_icon_from_key("driver_custom_action"), None);
    }
}
