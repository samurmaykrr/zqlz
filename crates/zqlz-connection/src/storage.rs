//! Secure storage for sensitive credentials using the system keychain
//!
//! This module provides secure credential storage using:
//! - macOS: Keychain
//! - Windows: Credential Manager
//! - Linux: Secret Service (via libsecret/GNOME Keyring)
//!
//! All credentials are stored in a SINGLE keychain entry as a JSON map.
//! This means the user only needs to click "Always Allow" once on macOS,
//! rather than once per connection.

use parking_lot::RwLock;
use std::collections::HashMap;
use uuid::Uuid;
use zqlz_core::{Result, ZqlzError};

/// Service name used for the keychain entry
const SERVICE_NAME: &str = "dev.zqlz.connections";

/// Account name for the single keychain entry that stores all credentials
const ACCOUNT_NAME: &str = "credentials";

/// Secure storage for credentials using the system keychain
///
/// Uses a single keychain entry to store all credentials as a JSON map.
/// This avoids multiple macOS Keychain permission prompts.
pub struct SecureStorage {
    /// In-memory cache of all credentials (loaded from keychain on first access)
    /// Map of "type:uuid" -> secret value
    cache: RwLock<Option<HashMap<String, String>>>,
    /// Whether to use fallback storage (in-memory only, no persistence)
    use_fallback: bool,
}

impl SecureStorage {
    /// Create a new secure storage instance
    ///
    /// Note: This does NOT access the keychain immediately. Credentials are
    /// loaded lazily on first access to avoid unnecessary permission prompts.
    pub fn new() -> Result<Self> {
        // We assume keychain is available and handle errors gracefully
        let use_fallback = false;

        tracing::debug!("Secure storage initialized (credentials loaded on-demand)");

        Ok(Self {
            cache: RwLock::new(None),
            use_fallback,
        })
    }

    /// Build the key for a connection's password
    fn password_key(connection_id: Uuid) -> String {
        format!("password:{}", connection_id)
    }

    /// Build the key for a connection's SSH key passphrase
    fn ssh_passphrase_key(connection_id: Uuid) -> String {
        format!("ssh_passphrase:{}", connection_id)
    }

    /// Store a connection's password securely
    pub fn store_password(&self, connection_id: Uuid, password: &str) -> Result<()> {
        let key = Self::password_key(connection_id);
        self.store(&key, password)
    }

    /// Retrieve a connection's password
    pub fn get_password(&self, connection_id: Uuid) -> Result<Option<String>> {
        let key = Self::password_key(connection_id);
        self.retrieve(&key)
    }

    /// Delete a connection's password
    pub fn delete_password(&self, connection_id: Uuid) -> Result<()> {
        let key = Self::password_key(connection_id);
        self.delete(&key)
    }

    /// Store a connection's SSH key passphrase securely
    pub fn store_ssh_passphrase(&self, connection_id: Uuid, passphrase: &str) -> Result<()> {
        let key = Self::ssh_passphrase_key(connection_id);
        self.store(&key, passphrase)
    }

    /// Retrieve a connection's SSH key passphrase
    pub fn get_ssh_passphrase(&self, connection_id: Uuid) -> Result<Option<String>> {
        let key = Self::ssh_passphrase_key(connection_id);
        self.retrieve(&key)
    }

    /// Delete a connection's SSH key passphrase
    pub fn delete_ssh_passphrase(&self, connection_id: Uuid) -> Result<()> {
        let key = Self::ssh_passphrase_key(connection_id);
        self.delete(&key)
    }

    /// Delete all credentials for a connection
    pub fn delete_connection_credentials(&self, connection_id: Uuid) -> Result<()> {
        // Delete password (ignore errors if not found)
        let _ = self.delete_password(connection_id);
        // Delete SSH passphrase (ignore errors if not found)
        let _ = self.delete_ssh_passphrase(connection_id);
        Ok(())
    }

    /// Load all credentials from keychain into cache
    fn load_from_keychain(&self) -> Result<HashMap<String, String>> {
        if self.use_fallback {
            return Ok(HashMap::new());
        }

        let entry = keyring::Entry::new(SERVICE_NAME, ACCOUNT_NAME)
            .map_err(|e| ZqlzError::Security(format!("Failed to create keyring entry: {}", e)))?;

        match entry.get_password() {
            Ok(json_str) => {
                let credentials: HashMap<String, String> = serde_json::from_str(&json_str)
                    .unwrap_or_else(|e| {
                        tracing::warn!("Failed to parse credentials JSON, starting fresh: {}", e);
                        HashMap::new()
                    });
                tracing::debug!(
                    count = credentials.len(),
                    "loaded credentials from keychain"
                );
                Ok(credentials)
            }
            Err(keyring::Error::NoEntry) => {
                tracing::debug!("no credentials found in keychain, starting fresh");
                Ok(HashMap::new())
            }
            Err(e) => {
                tracing::warn!("Failed to access keychain: {}, using empty credentials", e);
                // Don't fail - just return empty and use in-memory
                Ok(HashMap::new())
            }
        }
    }

    /// Save all credentials from cache to keychain
    fn save_to_keychain(&self, credentials: &HashMap<String, String>) -> Result<()> {
        if self.use_fallback {
            return Ok(());
        }

        let entry = keyring::Entry::new(SERVICE_NAME, ACCOUNT_NAME)
            .map_err(|e| ZqlzError::Security(format!("Failed to create keyring entry: {}", e)))?;

        if credentials.is_empty() {
            // Delete the entry if no credentials
            match entry.delete_credential() {
                Ok(()) => {
                    tracing::debug!("deleted empty credentials from keychain");
                }
                Err(keyring::Error::NoEntry) => {
                    // Already gone, that's fine
                }
                Err(e) => {
                    tracing::warn!("Failed to delete keychain entry: {}", e);
                }
            }
        } else {
            let json_str = serde_json::to_string(credentials).map_err(|e| {
                ZqlzError::Security(format!("Failed to serialize credentials: {}", e))
            })?;

            entry.set_password(&json_str).map_err(|e| {
                ZqlzError::Security(format!("Failed to store credentials in keychain: {}", e))
            })?;

            tracing::debug!(count = credentials.len(), "saved credentials to keychain");
        }

        Ok(())
    }

    /// Ensure cache is loaded
    fn ensure_loaded(&self) -> Result<()> {
        let mut cache = self.cache.write();
        if cache.is_none() {
            *cache = Some(self.load_from_keychain()?);
        }
        Ok(())
    }

    /// Store a secret
    fn store(&self, key: &str, value: &str) -> Result<()> {
        self.ensure_loaded()?;

        let mut cache = self.cache.write();
        let credentials = cache.as_mut().expect("cache should be loaded");

        credentials.insert(key.to_string(), value.to_string());

        // Save to keychain
        self.save_to_keychain(credentials)?;

        tracing::debug!(key = %key, "stored secret");
        Ok(())
    }

    /// Retrieve a secret
    fn retrieve(&self, key: &str) -> Result<Option<String>> {
        self.ensure_loaded()?;

        let cache = self.cache.read();
        let credentials = cache.as_ref().expect("cache should be loaded");

        let value = credentials.get(key).cloned();
        tracing::debug!(key = %key, found = value.is_some(), "retrieved secret");
        Ok(value)
    }

    /// Delete a secret
    fn delete(&self, key: &str) -> Result<()> {
        self.ensure_loaded()?;

        let mut cache = self.cache.write();
        let credentials = cache.as_mut().expect("cache should be loaded");

        if credentials.remove(key).is_some() {
            // Save to keychain
            self.save_to_keychain(credentials)?;
            tracing::debug!(key = %key, "deleted secret");
        } else {
            tracing::debug!(key = %key, "secret not found (nothing to delete)");
        }

        Ok(())
    }

    /// Check if using fallback storage (for diagnostics)
    pub fn is_using_fallback(&self) -> bool {
        self.use_fallback
    }

    /// Migrate credentials from old per-connection entries to new single entry
    ///
    /// Call this once during app startup to migrate existing credentials.
    /// It will check for old-style entries and move them to the new format.
    pub fn migrate_legacy_entries(&self, connection_ids: &[Uuid]) -> Result<()> {
        if self.use_fallback {
            return Ok(());
        }

        let mut migrated_count = 0;

        for conn_id in connection_ids {
            // Check for old-style password entry
            let old_password_key = format!("password:{}", conn_id);
            if let Ok(entry) = keyring::Entry::new(SERVICE_NAME, &old_password_key) {
                if let Ok(password) = entry.get_password() {
                    // Migrate to new storage
                    self.store_password(*conn_id, &password)?;
                    // Delete old entry
                    let _ = entry.delete_credential();
                    migrated_count += 1;
                    tracing::info!(connection_id = %conn_id, "migrated password to new storage format");
                }
            }

            // Check for old-style SSH passphrase entry
            let old_ssh_key = format!("ssh_passphrase:{}", conn_id);
            if let Ok(entry) = keyring::Entry::new(SERVICE_NAME, &old_ssh_key) {
                if let Ok(passphrase) = entry.get_password() {
                    // Migrate to new storage
                    self.store_ssh_passphrase(*conn_id, &passphrase)?;
                    // Delete old entry
                    let _ = entry.delete_credential();
                    migrated_count += 1;
                    tracing::info!(connection_id = %conn_id, "migrated SSH passphrase to new storage format");
                }
            }
        }

        if migrated_count > 0 {
            tracing::info!(
                count = migrated_count,
                "migrated credentials to new single-entry format"
            );
        }

        Ok(())
    }
}

impl Default for SecureStorage {
    fn default() -> Self {
        Self::new().expect("Failed to create secure storage")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_key_format() {
        let id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        assert_eq!(
            SecureStorage::password_key(id),
            "password:12345678-1234-1234-1234-123456789abc"
        );
    }

    #[test]
    fn test_ssh_passphrase_key_format() {
        let id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        assert_eq!(
            SecureStorage::ssh_passphrase_key(id),
            "ssh_passphrase:12345678-1234-1234-1234-123456789abc"
        );
    }
}
