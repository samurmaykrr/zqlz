use super::*;
use zqlz_core::DiagnosticSeverity as CoreSeverity;

#[test]
fn test_get_valid_command() {
    let validator = RedisValidator::new();
    let spec = validator.get_command("SET").unwrap();
    assert_eq!(spec.name, "SET");
    assert_eq!(spec.arity, -3);
    assert_eq!(spec.min_args(), 2);
    assert!(spec.max_args().is_none());
}

#[test]
fn test_exact_arity_command() {
    let validator = RedisValidator::new();
    let spec = validator.get_command("GET").unwrap();
    assert_eq!(spec.arity, 2);
    assert_eq!(spec.min_args(), 1);
    assert_eq!(spec.max_args(), Some(1));
}

#[test]
fn test_validate_missing_args() {
    let validator = RedisValidator::new();
    let errors = validator.validate("SET key", false);
    assert_eq!(errors.len(), 1);
    assert!(errors[0].message.contains("at least 2"));
}

#[test]
fn test_validate_exact_args_too_many() {
    let validator = RedisValidator::new();
    let errors = validator.validate("GET key extra", false);
    assert_eq!(errors.len(), 1);
    assert!(errors[0].message.contains("exactly 1"));
}

#[test]
fn test_validate_unknown_command() {
    let validator = RedisValidator::new();
    let errors = validator.validate("NOTACOMMAND arg", false);
    assert_eq!(errors.len(), 1);
    assert!(errors[0].message.contains("Unknown"));
}

#[test]
fn test_validate_deprecated_command() {
    let validator = RedisValidator::new();
    let errors = validator.validate("GETSET key value", false);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].severity, CoreSeverity::Warning);
    assert!(errors[0].message.contains("deprecated"));
}

#[test]
fn test_validate_valid_command() {
    let validator = RedisValidator::new();
    let errors = validator.validate("SET mykey myvalue", false);
    assert!(errors.is_empty());
}

#[test]
fn test_validate_multi_line() {
    let validator = RedisValidator::new();
    let errors = validator.validate("SET key value\nGET\nHSET hash field", false);
    assert_eq!(errors.len(), 2);
}

#[test]
fn test_validate_quoted_strings() {
    let validator = RedisValidator::new();
    let errors = validator.validate(r#"SET "my key" "my value""#, false);
    assert!(errors.is_empty());
}

#[test]
fn test_validate_no_args_command() {
    let validator = RedisValidator::new();
    let errors = validator.validate("MULTI", false);
    assert!(errors.is_empty());
}

#[test]
fn test_total_command_count() {
    let validator = RedisValidator::new();
    assert!(
        validator.commands.len() >= 350,
        "Expected at least 350 commands, got {}",
        validator.commands.len()
    );
}

#[test]
fn test_stream_commands() {
    let validator = RedisValidator::new();
    let spec = validator.get_command("XADD").unwrap();
    assert_eq!(spec.arity, -5);
    assert_eq!(spec.group, "stream");

    let errors = validator.validate("XADD mystream * field1 value1", false);
    assert!(errors.is_empty());

    let errors = validator.validate("XADD mystream *", false);
    assert_eq!(errors.len(), 1);
}

#[test]
fn test_cluster_subcommands() {
    let validator = RedisValidator::new();

    let spec = validator.get_command("CLUSTER INFO").unwrap();
    assert_eq!(spec.arity, 2);

    let spec = validator.get_command("CLUSTER NODES").unwrap();
    assert_eq!(spec.arity, 2);
}

#[test]
fn test_geo_commands() {
    let validator = RedisValidator::new();

    let spec = validator.get_command("GEOADD").unwrap();
    assert_eq!(spec.arity, -5);
    assert_eq!(spec.group, "geo");

    assert!(validator.get_command("GEOSEARCH").is_some());

    let spec = validator.get_command("GEORADIUS").unwrap();
    assert!(spec.deprecated);
}

#[test]
fn test_function_commands() {
    let validator = RedisValidator::new();

    let spec = validator.get_command("FUNCTION LOAD").unwrap();
    assert_eq!(spec.group, "scripting");

    let spec = validator.get_command("FCALL").unwrap();
    assert_eq!(spec.arity, -3);
}

#[test]
fn test_acl_commands() {
    let validator = RedisValidator::new();

    assert!(validator.get_command("ACL").is_some());
    assert!(validator.get_command("ACL LIST").is_some());
    assert!(validator.get_command("ACL WHOAMI").is_some());
    assert!(validator.get_command("ACL SETUSER").is_some());
}

#[test]
fn test_deprecated_commands_have_replacements() {
    let validator = RedisValidator::new();

    let deprecated_with_replacements = [
        ("GETSET", "SET with GET option"),
        ("SETNX", "SET with NX option"),
        ("SETEX", "SET with EX option"),
        ("HMSET", "HSET"),
        ("ZRANGEBYSCORE", "ZRANGE with BYSCORE"),
        ("ZREVRANGE", "ZRANGE with REV"),
        ("SLAVEOF", "REPLICAOF"),
        ("BRPOPLPUSH", "BLMOVE with RIGHT and LEFT"),
    ];

    for (command, expected_replacement) in deprecated_with_replacements {
        let spec = validator.get_command(command).unwrap();
        assert!(spec.deprecated, "{} should be deprecated", command);
        assert!(
            spec.replaced_by
                .as_ref()
                .unwrap()
                .contains(expected_replacement)
                || spec.replaced_by.as_ref().unwrap() == expected_replacement,
            "{} should be replaced by something containing '{}', got {:?}",
            command,
            expected_replacement,
            spec.replaced_by
        );
    }
}

#[test]
fn test_hyperloglog_commands() {
    let validator = RedisValidator::new();

    assert!(validator.get_command("PFADD").is_some());
    assert!(validator.get_command("PFCOUNT").is_some());
    assert!(validator.get_command("PFMERGE").is_some());

    let errors = validator.validate("PFADD hll a b c", false);
    assert!(errors.is_empty());
}

#[test]
fn test_bitmap_commands() {
    let validator = RedisValidator::new();

    assert!(validator.get_command("BITCOUNT").is_some());
    assert!(validator.get_command("BITFIELD").is_some());
    assert!(validator.get_command("BITOP").is_some());
    assert!(validator.get_command("BITPOS").is_some());
    assert!(validator.get_command("GETBIT").is_some());
    assert!(validator.get_command("SETBIT").is_some());
}

#[test]
fn test_client_subcommands() {
    let validator = RedisValidator::new();

    assert!(validator.get_command("CLIENT").is_some());
    assert!(validator.get_command("CLIENT ID").is_some());
    assert!(validator.get_command("CLIENT LIST").is_some());
    assert!(validator.get_command("CLIENT KILL").is_some());
    assert!(validator.get_command("CLIENT SETNAME").is_some());
    assert!(validator.get_command("CLIENT TRACKING").is_some());
}
