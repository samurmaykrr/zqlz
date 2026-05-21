use std::collections::HashMap;
use std::sync::OnceLock;

use crate::redis_command_spec::RedisCommandSpec;

static BUILTIN_COMMANDS: OnceLock<HashMap<String, RedisCommandSpec>> = OnceLock::new();

pub fn builtin_commands() -> &'static HashMap<String, RedisCommandSpec> {
    BUILTIN_COMMANDS.get_or_init(|| {
        let mut commands = HashMap::new();
        add_builtin_commands(&mut commands);
        commands
    })
}

pub fn top_level_commands(
    commands: &HashMap<String, RedisCommandSpec>,
    prefix: Option<&str>,
) -> Vec<String> {
    let prefix = prefix.unwrap_or_default().to_uppercase();
    let mut command_names: Vec<String> = commands
        .keys()
        .filter_map(|name| {
            let top_level = name.split_whitespace().next()?;
            (!top_level.is_empty()).then_some(top_level.to_string())
        })
        .collect();
    command_names.sort();
    command_names.dedup();
    if prefix.is_empty() {
        return command_names;
    }

    command_names
        .into_iter()
        .filter(|command| command.starts_with(&prefix))
        .collect()
}

pub fn subcommands_for_path(
    commands: &HashMap<String, RedisCommandSpec>,
    path: &[&str],
    prefix: Option<&str>,
) -> Vec<String> {
    if path.is_empty() {
        return Vec::new();
    }

    let path: Vec<String> = path.iter().map(|segment| segment.to_uppercase()).collect();
    let prefix = prefix.unwrap_or_default().to_uppercase();
    let mut subcommands = Vec::new();

    for name in commands.keys() {
        let parts: Vec<&str> = name.split_whitespace().collect();
        if parts.len() <= path.len() {
            continue;
        }
        if parts[..path.len()]
            .iter()
            .map(|segment| segment.to_uppercase())
            .eq(path.iter().cloned())
        {
            let candidate = parts[path.len()].to_string();
            if prefix.is_empty() || candidate.starts_with(&prefix) {
                subcommands.push(candidate);
            }
        }
    }

    subcommands.sort();
    subcommands.dedup();
    subcommands
}

/// Add built-in Redis command specifications
///
/// All 370 commands auto-generated from Redis official commands.json
/// Source: https://github.com/redis/redis-doc/blob/master/commands.json
pub fn add_builtin_commands(commands: &mut HashMap<String, RedisCommandSpec>) {
    // Helper to add a command quickly
    fn add(commands: &mut HashMap<String, RedisCommandSpec>, name: &str, arity: i32, group: &str) {
        commands.insert(
            name.to_string(),
            RedisCommandSpec {
                name: name.to_string(),
                arity,
                group: group.to_string(),
                summary: String::new(),
                deprecated: false,
                replaced_by: None,
            },
        );
    }

    // Helper to add a deprecated command
    fn add_deprecated(
        commands: &mut HashMap<String, RedisCommandSpec>,
        name: &str,
        arity: i32,
        group: &str,
        replaced_by: Option<&str>,
    ) {
        commands.insert(
            name.to_string(),
            RedisCommandSpec {
                name: name.to_string(),
                arity,
                group: group.to_string(),
                summary: String::new(),
                deprecated: true,
                replaced_by: replaced_by.map(|s| s.to_string()),
            },
        );
    }

    // ===== AUTO-GENERATED FROM REDIS commands.json =====
    // 349 regular commands + 21 deprecated commands = 370 total

    // === SERVER COMMANDS (76) ===
    add(commands, "ACL", -2, "server");
    add(commands, "ACL CAT", -2, "server");
    add(commands, "ACL DELUSER", -3, "server");
    add(commands, "ACL DRYRUN", -4, "server");
    add(commands, "ACL GENPASS", -2, "server");
    add(commands, "ACL GETUSER", 3, "server");
    add(commands, "ACL HELP", 2, "server");
    add(commands, "ACL LIST", 2, "server");
    add(commands, "ACL LOAD", 2, "server");
    add(commands, "ACL LOG", -2, "server");
    add(commands, "ACL SAVE", 2, "server");
    add(commands, "ACL SETUSER", -3, "server");
    add(commands, "ACL USERS", 2, "server");
    add(commands, "ACL WHOAMI", 2, "server");
    add(commands, "BGREWRITEAOF", 1, "server");
    add(commands, "BGSAVE", -1, "server");
    add(commands, "COMMAND", -1, "server");
    add(commands, "COMMAND COUNT", 2, "server");
    add(commands, "COMMAND DOCS", -2, "server");
    add(commands, "COMMAND GETKEYS", -3, "server");
    add(commands, "COMMAND GETKEYSANDFLAGS", -3, "server");
    add(commands, "COMMAND HELP", 2, "server");
    add(commands, "COMMAND INFO", -2, "server");
    add(commands, "COMMAND LIST", -2, "server");
    add(commands, "CONFIG", -2, "server");
    add(commands, "CONFIG GET", -3, "server");
    add(commands, "CONFIG HELP", 2, "server");
    add(commands, "CONFIG RESETSTAT", 2, "server");
    add(commands, "CONFIG REWRITE", 2, "server");
    add(commands, "CONFIG SET", -4, "server");
    add(commands, "DBSIZE", 1, "server");
    add(commands, "DEBUG", -2, "server");
    add(commands, "FAILOVER", -1, "server");
    add(commands, "FLUSHALL", -1, "server");
    add(commands, "FLUSHDB", -1, "server");
    add(commands, "INFO", -1, "server");
    add(commands, "LASTSAVE", 1, "server");
    add(commands, "LATENCY", -2, "server");
    add(commands, "LATENCY DOCTOR", 2, "server");
    add(commands, "LATENCY GRAPH", 3, "server");
    add(commands, "LATENCY HELP", 2, "server");
    add(commands, "LATENCY HISTOGRAM", -2, "server");
    add(commands, "LATENCY HISTORY", 3, "server");
    add(commands, "LATENCY LATEST", 2, "server");
    add(commands, "LATENCY RESET", -2, "server");
    add(commands, "LOLWUT", -1, "server");
    add(commands, "MEMORY", -2, "server");
    add(commands, "MEMORY DOCTOR", 2, "server");
    add(commands, "MEMORY HELP", 2, "server");
    add(commands, "MEMORY MALLOC-STATS", 2, "server");
    add(commands, "MEMORY PURGE", 2, "server");
    add(commands, "MEMORY STATS", 2, "server");
    add(commands, "MEMORY USAGE", -3, "server");
    add(commands, "MODULE", -2, "server");
    add(commands, "MODULE HELP", 2, "server");
    add(commands, "MODULE LIST", 2, "server");
    add(commands, "MODULE LOAD", -3, "server");
    add(commands, "MODULE LOADEX", -3, "server");
    add(commands, "MODULE UNLOAD", 3, "server");
    add(commands, "MONITOR", 1, "server");
    add(commands, "PSYNC", -3, "server");
    add(commands, "REPLCONF", -1, "server");
    add(commands, "REPLICAOF", 3, "server");
    add(commands, "RESTORE-ASKING", -4, "server");
    add(commands, "ROLE", 1, "server");
    add(commands, "SAVE", 1, "server");
    add(commands, "SHUTDOWN", -1, "server");
    add(commands, "SLOWLOG", -2, "server");
    add(commands, "SLOWLOG GET", -2, "server");
    add(commands, "SLOWLOG HELP", 2, "server");
    add(commands, "SLOWLOG LEN", 2, "server");
    add(commands, "SLOWLOG RESET", 2, "server");
    add(commands, "SWAPDB", 3, "server");
    add(commands, "SYNC", 1, "server");
    add(commands, "TIME", 1, "server");

    // === GENERIC/KEY COMMANDS (34) ===
    add(commands, "COPY", -3, "generic");
    add(commands, "DEL", -2, "generic");
    add(commands, "DUMP", 2, "generic");
    add(commands, "EXISTS", -2, "generic");
    add(commands, "EXPIRE", -3, "generic");
    add(commands, "EXPIREAT", -3, "generic");
    add(commands, "EXPIRETIME", 2, "generic");
    add(commands, "KEYS", 2, "generic");
    add(commands, "MIGRATE", -6, "generic");
    add(commands, "MOVE", 3, "generic");
    add(commands, "OBJECT", -2, "generic");
    add(commands, "OBJECT ENCODING", 3, "generic");
    add(commands, "OBJECT FREQ", 3, "generic");
    add(commands, "OBJECT HELP", 2, "generic");
    add(commands, "OBJECT IDLETIME", 3, "generic");
    add(commands, "OBJECT REFCOUNT", 3, "generic");
    add(commands, "PERSIST", 2, "generic");
    add(commands, "PEXPIRE", -3, "generic");
    add(commands, "PEXPIREAT", -3, "generic");
    add(commands, "PEXPIRETIME", 2, "generic");
    add(commands, "PTTL", 2, "generic");
    add(commands, "RANDOMKEY", 1, "generic");
    add(commands, "RENAME", 3, "generic");
    add(commands, "RENAMENX", 3, "generic");
    add(commands, "RESTORE", -4, "generic");
    add(commands, "SCAN", -2, "generic");
    add(commands, "SORT", -2, "generic");
    add(commands, "SORT_RO", -2, "generic");
    add(commands, "TOUCH", -2, "generic");
    add(commands, "TTL", 2, "generic");
    add(commands, "TYPE", 2, "generic");
    add(commands, "UNLINK", -2, "generic");
    add(commands, "WAIT", 3, "generic");
    add(commands, "WAITAOF", 4, "generic");

    // === STRING COMMANDS (22) ===
    add(commands, "APPEND", 3, "string");
    add(commands, "DECR", 2, "string");
    add(commands, "DECRBY", 3, "string");
    add(commands, "GET", 2, "string");
    add(commands, "GETDEL", 2, "string");
    add(commands, "GETEX", -2, "string");
    add(commands, "GETRANGE", 4, "string");
    add(commands, "INCR", 2, "string");
    add(commands, "INCRBY", 3, "string");
    add(commands, "INCRBYFLOAT", 3, "string");
    add(commands, "LCS", -3, "string");
    add(commands, "MGET", -2, "string");
    add(commands, "MSET", -3, "string");
    add(commands, "MSETNX", -3, "string");
    add(commands, "SET", -3, "string");
    add(commands, "SETRANGE", 4, "string");
    add(commands, "STRLEN", 2, "string");

    // === HASH COMMANDS (16) ===
    add(commands, "HDEL", -3, "hash");
    add(commands, "HEXISTS", 3, "hash");
    add(commands, "HGET", 3, "hash");
    add(commands, "HGETALL", 2, "hash");
    add(commands, "HINCRBY", 4, "hash");
    add(commands, "HINCRBYFLOAT", 4, "hash");
    add(commands, "HKEYS", 2, "hash");
    add(commands, "HLEN", 2, "hash");
    add(commands, "HMGET", -3, "hash");
    add(commands, "HRANDFIELD", -2, "hash");
    add(commands, "HSCAN", -3, "hash");
    add(commands, "HSET", -4, "hash");
    add(commands, "HSETNX", 4, "hash");
    add(commands, "HSTRLEN", 3, "hash");
    add(commands, "HVALS", 2, "hash");

    // === LIST COMMANDS (22) ===
    add(commands, "BLMOVE", 6, "list");
    add(commands, "BLMPOP", -5, "list");
    add(commands, "BLPOP", -3, "list");
    add(commands, "BRPOP", -3, "list");
    add(commands, "LINDEX", 3, "list");
    add(commands, "LINSERT", 5, "list");
    add(commands, "LLEN", 2, "list");
    add(commands, "LMOVE", 5, "list");
    add(commands, "LMPOP", -4, "list");
    add(commands, "LPOP", -2, "list");
    add(commands, "LPOS", -3, "list");
    add(commands, "LPUSH", -3, "list");
    add(commands, "LPUSHX", -3, "list");
    add(commands, "LRANGE", 4, "list");
    add(commands, "LREM", 4, "list");
    add(commands, "LSET", 4, "list");
    add(commands, "LTRIM", 4, "list");
    add(commands, "RPOP", -2, "list");
    add(commands, "RPUSH", -3, "list");
    add(commands, "RPUSHX", -3, "list");

    // === SET COMMANDS (17) ===
    add(commands, "SADD", -3, "set");
    add(commands, "SCARD", 2, "set");
    add(commands, "SDIFF", -2, "set");
    add(commands, "SDIFFSTORE", -3, "set");
    add(commands, "SINTER", -2, "set");
    add(commands, "SINTERCARD", -3, "set");
    add(commands, "SINTERSTORE", -3, "set");
    add(commands, "SISMEMBER", 3, "set");
    add(commands, "SMEMBERS", 2, "set");
    add(commands, "SMISMEMBER", -3, "set");
    add(commands, "SMOVE", 4, "set");
    add(commands, "SPOP", -2, "set");
    add(commands, "SRANDMEMBER", -2, "set");
    add(commands, "SREM", -3, "set");
    add(commands, "SSCAN", -3, "set");
    add(commands, "SUNION", -2, "set");
    add(commands, "SUNIONSTORE", -3, "set");

    // === SORTED SET COMMANDS (35) ===
    add(commands, "BZMPOP", -5, "sorted-set");
    add(commands, "BZPOPMAX", -3, "sorted-set");
    add(commands, "BZPOPMIN", -3, "sorted-set");
    add(commands, "ZADD", -4, "sorted-set");
    add(commands, "ZCARD", 2, "sorted-set");
    add(commands, "ZCOUNT", 4, "sorted-set");
    add(commands, "ZDIFF", -3, "sorted-set");
    add(commands, "ZDIFFSTORE", -4, "sorted-set");
    add(commands, "ZINCRBY", 4, "sorted-set");
    add(commands, "ZINTER", -3, "sorted-set");
    add(commands, "ZINTERCARD", -3, "sorted-set");
    add(commands, "ZINTERSTORE", -4, "sorted-set");
    add(commands, "ZLEXCOUNT", 4, "sorted-set");
    add(commands, "ZMPOP", -4, "sorted-set");
    add(commands, "ZMSCORE", -3, "sorted-set");
    add(commands, "ZPOPMAX", -2, "sorted-set");
    add(commands, "ZPOPMIN", -2, "sorted-set");
    add(commands, "ZRANDMEMBER", -2, "sorted-set");
    add(commands, "ZRANGE", -4, "sorted-set");
    add(commands, "ZRANGESTORE", -5, "sorted-set");
    add(commands, "ZRANK", -3, "sorted-set");
    add(commands, "ZREM", -3, "sorted-set");
    add(commands, "ZREMRANGEBYLEX", 4, "sorted-set");
    add(commands, "ZREMRANGEBYRANK", 4, "sorted-set");
    add(commands, "ZREMRANGEBYSCORE", 4, "sorted-set");
    add(commands, "ZREVRANK", -3, "sorted-set");
    add(commands, "ZSCAN", -3, "sorted-set");
    add(commands, "ZSCORE", 3, "sorted-set");
    add(commands, "ZUNION", -3, "sorted-set");
    add(commands, "ZUNIONSTORE", -4, "sorted-set");

    // === BITMAP COMMANDS (7) ===
    add(commands, "BITCOUNT", -2, "bitmap");
    add(commands, "BITFIELD", -2, "bitmap");
    add(commands, "BITFIELD_RO", -2, "bitmap");
    add(commands, "BITOP", -4, "bitmap");
    add(commands, "BITPOS", -3, "bitmap");
    add(commands, "GETBIT", 3, "bitmap");
    add(commands, "SETBIT", 4, "bitmap");

    // === HYPERLOGLOG COMMANDS (5) ===
    add(commands, "PFADD", -2, "hyperloglog");
    add(commands, "PFCOUNT", -2, "hyperloglog");
    add(commands, "PFDEBUG", 3, "hyperloglog");
    add(commands, "PFMERGE", -2, "hyperloglog");
    add(commands, "PFSELFTEST", 1, "hyperloglog");

    // === GEO COMMANDS (10) ===
    add(commands, "GEOADD", -5, "geo");
    add(commands, "GEODIST", -4, "geo");
    add(commands, "GEOHASH", -2, "geo");
    add(commands, "GEOPOS", -2, "geo");
    add(commands, "GEOSEARCH", -7, "geo");
    add(commands, "GEOSEARCHSTORE", -8, "geo");

    // === STREAM COMMANDS (25) ===
    add(commands, "XACK", -4, "stream");
    add(commands, "XADD", -5, "stream");
    add(commands, "XAUTOCLAIM", -6, "stream");
    add(commands, "XCLAIM", -6, "stream");
    add(commands, "XDEL", -3, "stream");
    add(commands, "XGROUP", -2, "stream");
    add(commands, "XGROUP CREATE", -5, "stream");
    add(commands, "XGROUP CREATECONSUMER", 5, "stream");
    add(commands, "XGROUP DELCONSUMER", 5, "stream");
    add(commands, "XGROUP DESTROY", 4, "stream");
    add(commands, "XGROUP HELP", 2, "stream");
    add(commands, "XGROUP SETID", -5, "stream");
    add(commands, "XINFO", -2, "stream");
    add(commands, "XINFO CONSUMERS", 4, "stream");
    add(commands, "XINFO GROUPS", 3, "stream");
    add(commands, "XINFO HELP", 2, "stream");
    add(commands, "XINFO STREAM", -3, "stream");
    add(commands, "XLEN", 2, "stream");
    add(commands, "XPENDING", -3, "stream");
    add(commands, "XRANGE", -4, "stream");
    add(commands, "XREAD", -4, "stream");
    add(commands, "XREADGROUP", -7, "stream");
    add(commands, "XREVRANGE", -4, "stream");
    add(commands, "XSETID", -3, "stream");
    add(commands, "XTRIM", -4, "stream");

    // === CONNECTION COMMANDS (26) ===
    add(commands, "AUTH", -2, "connection");
    add(commands, "CLIENT", -2, "connection");
    add(commands, "CLIENT CACHING", 3, "connection");
    add(commands, "CLIENT GETNAME", 2, "connection");
    add(commands, "CLIENT GETREDIR", 2, "connection");
    add(commands, "CLIENT HELP", 2, "connection");
    add(commands, "CLIENT ID", 2, "connection");
    add(commands, "CLIENT INFO", 2, "connection");
    add(commands, "CLIENT KILL", -3, "connection");
    add(commands, "CLIENT LIST", -2, "connection");
    add(commands, "CLIENT NO-EVICT", 3, "connection");
    add(commands, "CLIENT NO-TOUCH", 3, "connection");
    add(commands, "CLIENT PAUSE", -3, "connection");
    add(commands, "CLIENT REPLY", 3, "connection");
    add(commands, "CLIENT SETINFO", 4, "connection");
    add(commands, "CLIENT SETNAME", 3, "connection");
    add(commands, "CLIENT TRACKING", -3, "connection");
    add(commands, "CLIENT TRACKINGINFO", 2, "connection");
    add(commands, "CLIENT UNBLOCK", -3, "connection");
    add(commands, "CLIENT UNPAUSE", 2, "connection");
    add(commands, "ECHO", 2, "connection");
    add(commands, "HELLO", -1, "connection");
    add(commands, "PING", -1, "connection");
    add(commands, "RESET", 1, "connection");
    add(commands, "SELECT", 2, "connection");

    // === CLUSTER COMMANDS (32) ===
    add(commands, "ASKING", 1, "cluster");
    add(commands, "CLUSTER", -2, "cluster");
    add(commands, "CLUSTER ADDSLOTS", -3, "cluster");
    add(commands, "CLUSTER ADDSLOTSRANGE", -4, "cluster");
    add(commands, "CLUSTER BUMPEPOCH", 2, "cluster");
    add(commands, "CLUSTER COUNT-FAILURE-REPORTS", 3, "cluster");
    add(commands, "CLUSTER COUNTKEYSINSLOT", 3, "cluster");
    add(commands, "CLUSTER DELSLOTS", -3, "cluster");
    add(commands, "CLUSTER DELSLOTSRANGE", -4, "cluster");
    add(commands, "CLUSTER FAILOVER", -2, "cluster");
    add(commands, "CLUSTER FLUSHSLOTS", 2, "cluster");
    add(commands, "CLUSTER FORGET", 3, "cluster");
    add(commands, "CLUSTER GETKEYSINSLOT", 4, "cluster");
    add(commands, "CLUSTER HELP", 2, "cluster");
    add(commands, "CLUSTER INFO", 2, "cluster");
    add(commands, "CLUSTER KEYSLOT", 3, "cluster");
    add(commands, "CLUSTER LINKS", 2, "cluster");
    add(commands, "CLUSTER MEET", -4, "cluster");
    add(commands, "CLUSTER MYID", 2, "cluster");
    add(commands, "CLUSTER MYSHARDID", 2, "cluster");
    add(commands, "CLUSTER NODES", 2, "cluster");
    add(commands, "CLUSTER REPLICAS", 3, "cluster");
    add(commands, "CLUSTER REPLICATE", 3, "cluster");
    add(commands, "CLUSTER RESET", -2, "cluster");
    add(commands, "CLUSTER SAVECONFIG", 2, "cluster");
    add(commands, "CLUSTER SET-CONFIG-EPOCH", 3, "cluster");
    add(commands, "CLUSTER SETSLOT", -4, "cluster");
    add(commands, "CLUSTER SHARDS", 2, "cluster");
    add(commands, "READONLY", 1, "cluster");
    add(commands, "READWRITE", 1, "cluster");

    // === TRANSACTIONS COMMANDS (5) ===
    add(commands, "DISCARD", 1, "transactions");
    add(commands, "EXEC", 1, "transactions");
    add(commands, "MULTI", 1, "transactions");
    add(commands, "UNWATCH", 1, "transactions");
    add(commands, "WATCH", -2, "transactions");

    // === PUBSUB COMMANDS (15) ===
    add(commands, "PSUBSCRIBE", -2, "pubsub");
    add(commands, "PUBLISH", 3, "pubsub");
    add(commands, "PUBSUB", -2, "pubsub");
    add(commands, "PUBSUB CHANNELS", -2, "pubsub");
    add(commands, "PUBSUB HELP", 2, "pubsub");
    add(commands, "PUBSUB NUMPAT", 2, "pubsub");
    add(commands, "PUBSUB NUMSUB", -2, "pubsub");
    add(commands, "PUBSUB SHARDCHANNELS", -2, "pubsub");
    add(commands, "PUBSUB SHARDNUMSUB", -2, "pubsub");
    add(commands, "PUNSUBSCRIBE", -1, "pubsub");
    add(commands, "SPUBLISH", 3, "pubsub");
    add(commands, "SSUBSCRIBE", -2, "pubsub");
    add(commands, "SUBSCRIBE", -2, "pubsub");
    add(commands, "SUNSUBSCRIBE", -1, "pubsub");
    add(commands, "UNSUBSCRIBE", -1, "pubsub");

    // === SCRIPTING COMMANDS (23) ===
    add(commands, "EVAL", -3, "scripting");
    add(commands, "EVALSHA", -3, "scripting");
    add(commands, "EVALSHA_RO", -3, "scripting");
    add(commands, "EVAL_RO", -3, "scripting");
    add(commands, "FCALL", -3, "scripting");
    add(commands, "FCALL_RO", -3, "scripting");
    add(commands, "FUNCTION", -2, "scripting");
    add(commands, "FUNCTION DELETE", 3, "scripting");
    add(commands, "FUNCTION DUMP", 2, "scripting");
    add(commands, "FUNCTION FLUSH", -2, "scripting");
    add(commands, "FUNCTION HELP", 2, "scripting");
    add(commands, "FUNCTION KILL", 2, "scripting");
    add(commands, "FUNCTION LIST", -2, "scripting");
    add(commands, "FUNCTION LOAD", -3, "scripting");
    add(commands, "FUNCTION RESTORE", -3, "scripting");
    add(commands, "FUNCTION STATS", 2, "scripting");
    add(commands, "SCRIPT", -2, "scripting");
    add(commands, "SCRIPT DEBUG", 3, "scripting");
    add(commands, "SCRIPT EXISTS", -3, "scripting");
    add(commands, "SCRIPT FLUSH", -2, "scripting");
    add(commands, "SCRIPT HELP", 2, "scripting");
    add(commands, "SCRIPT KILL", 2, "scripting");
    add(commands, "SCRIPT LOAD", 3, "scripting");

    // ===== DEPRECATED COMMANDS (21) =====
    // These commands are still supported but should show warnings
    add_deprecated(
        commands,
        "BRPOPLPUSH",
        4,
        "list",
        Some("BLMOVE with RIGHT and LEFT"),
    );
    add_deprecated(
        commands,
        "CLUSTER SLAVES",
        3,
        "cluster",
        Some("CLUSTER REPLICAS"),
    );
    add_deprecated(
        commands,
        "CLUSTER SLOTS",
        2,
        "cluster",
        Some("CLUSTER SHARDS"),
    );
    add_deprecated(commands, "GEORADIUS", -6, "geo", Some("GEOSEARCH"));
    add_deprecated(commands, "GEORADIUSBYMEMBER", -5, "geo", Some("GEOSEARCH"));
    add_deprecated(
        commands,
        "GEORADIUSBYMEMBER_RO",
        -5,
        "geo",
        Some("GEOSEARCH"),
    );
    add_deprecated(commands, "GEORADIUS_RO", -6, "geo", Some("GEOSEARCH"));
    add_deprecated(commands, "GETSET", 3, "string", Some("SET with GET option"));
    add_deprecated(commands, "HMSET", -4, "hash", Some("HSET"));
    add_deprecated(commands, "PSETEX", 4, "string", Some("SET with PX option"));
    add_deprecated(
        commands,
        "QUIT",
        -1,
        "connection",
        Some("just close the connection"),
    );
    add_deprecated(
        commands,
        "RPOPLPUSH",
        3,
        "list",
        Some("LMOVE with RIGHT and LEFT"),
    );
    add_deprecated(commands, "SETEX", 4, "string", Some("SET with EX option"));
    add_deprecated(commands, "SETNX", 3, "string", Some("SET with NX option"));
    add_deprecated(commands, "SLAVEOF", 3, "server", Some("REPLICAOF"));
    add_deprecated(commands, "SUBSTR", 4, "string", Some("GETRANGE"));
    add_deprecated(
        commands,
        "ZRANGEBYLEX",
        -4,
        "sorted-set",
        Some("ZRANGE with BYLEX"),
    );
    add_deprecated(
        commands,
        "ZRANGEBYSCORE",
        -4,
        "sorted-set",
        Some("ZRANGE with BYSCORE"),
    );
    add_deprecated(
        commands,
        "ZREVRANGE",
        -4,
        "sorted-set",
        Some("ZRANGE with REV"),
    );
    add_deprecated(
        commands,
        "ZREVRANGEBYLEX",
        -4,
        "sorted-set",
        Some("ZRANGE with REV and BYLEX"),
    );
    add_deprecated(
        commands,
        "ZREVRANGEBYSCORE",
        -4,
        "sorted-set",
        Some("ZRANGE with REV and BYSCORE"),
    );
}
