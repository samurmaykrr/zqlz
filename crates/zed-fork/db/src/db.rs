// Stub for Zed's db crate

pub mod sqlez {
    pub mod migrations {
        pub trait Migration: Send + Sync {
            fn run(&self) -> anyhow::Result<()>;
        }
    }

    pub trait Queryable {
        fn query(&self, sql: &str) -> anyhow::Result<()>;
    }

    pub trait Bindable {
        fn bind(&self) -> Vec<u8>;
    }

    pub mod bindable {
        pub use super::Bindable;

        pub trait Bind {
            fn bind(
                &self,
                _stmt: &mut crate::sqlez::statement::Statement,
                _index: usize,
            ) -> anyhow::Result<()> {
                Ok(())
            }
        }

        pub trait Column {
            fn column(
                _stmt: &crate::sqlez::statement::Statement,
                _index: usize,
            ) -> anyhow::Result<Self>
            where
                Self: Sized;
        }

        pub trait StaticColumnCount {
            fn column_count() -> usize;
        }

        // Implement for common types
        impl Bind for String {}
        impl Bind for i64 {}
        impl Bind for f64 {}
        impl Bind for bool {}
    }

    pub mod domain {
        pub trait Domain {
            const NAME: &'static str = "default";
            const MIGRATIONS: &'static [&'static str] = &[];

            fn name(&self) -> &str {
                Self::NAME
            }
        }
    }

    pub mod statement {
        pub struct Statement;

        impl Statement {
            pub fn prepare(_sql: &str) -> anyhow::Result<Self> {
                Ok(Statement)
            }
        }
    }

    pub mod thread_safe_connection {
        pub struct ThreadSafeConnection;

        impl ThreadSafeConnection {
            pub fn write<F, R>(&self, f: F) -> anyhow::Result<R>
            where
                F: FnOnce() -> anyhow::Result<R>,
            {
                f()
            }
        }
    }
}

pub mod sqlez_macros {
    // Macro stubs - these won't actually work but prevent compilation errors
    pub use crate::query;

    #[macro_export]
    macro_rules! sql {
        ($($tt:tt)*) => {
            ""
        };
    }
}

pub mod static_connection {
    pub struct StaticConnection;
}

// Stub query macro - doesn't actually execute queries, just prevents compilation errors
#[macro_export]
macro_rules! query {
    ($($tt:tt)*) => {
        |_conn: &_| -> anyhow::Result<()> { Ok(()) }
    };
}
