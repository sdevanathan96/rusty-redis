//! Startup configuration from argv, in Redis's `--name value` style.

use std::fmt;

use crate::int::strict_i64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub net: NetConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetConfig {
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            net: NetConfig { port: 6379 },
        }
    }
}

/// Every way argv can be wrong. `flag` is the name without its leading `--`,
/// lowercased, which is how Redis matches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    UnknownFlag {
        flag: String,
    },
    /// A token before the first `--flag`. Redis would read it as a config
    /// file path; config files are not supported here.
    UnexpectedArgument {
        value: String,
    },
    WrongNumberOfArguments {
        flag: String,
    },
    NotAnInteger {
        flag: String,
        value: String,
    },
    OutOfRange {
        flag: String,
        value: String,
        min: i64,
        max: i64,
    },
    /// Port 0, which Redis reads as "no TCP listener". With no unix socket
    /// either, there is nowhere left to listen.
    NotListening,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::UnknownFlag { flag } => write!(f, "unknown flag --{flag}"),
            ConfigError::UnexpectedArgument { value } => write!(
                f,
                "unexpected argument \"{value}\" before any --flag (config files are not supported)"
            ),
            ConfigError::WrongNumberOfArguments { flag } => {
                write!(f, "--{flag}: wrong number of arguments")
            }
            ConfigError::NotAnInteger { flag, value } => {
                write!(
                    f,
                    "--{flag} \"{value}\": argument couldn't be parsed into an integer"
                )
            }
            ConfigError::OutOfRange {
                flag,
                value,
                min,
                max,
            } => write!(
                f,
                "--{flag} \"{value}\": argument must be between {min} and {max} inclusive"
            ),
            ConfigError::NotListening => write!(f, "configured to not listen anywhere"),
        }
    }
}

impl std::error::Error for ConfigError {}

pub fn parse(args: &[String]) -> Result<Config, ConfigError> {
    let mut config = Config::default();
    for (flag, values) in group(args)? {
        match flag.as_str() {
            "port" => config.net.port = port(&flag, &values)?,
            _ => return Err(ConfigError::UnknownFlag { flag }),
        }
    }
    // Checked after every flag is read, because the last --port wins.
    if config.net.port == 0 {
        return Err(ConfigError::NotListening);
    }
    Ok(config)
}

/// Splits argv the way Redis does: each `--name` starts a flag, and every token
/// up to the next `--name` is one of its arguments. So `--port 7000 extra` is
/// `port` with two arguments, not `port` followed by an unknown `extra`.
fn group(args: &[String]) -> Result<Vec<(String, Vec<String>)>, ConfigError> {
    let mut groups: Vec<(String, Vec<String>)> = Vec::new();
    for arg in args {
        match arg.strip_prefix("--") {
            Some(name) => groups.push((name.to_ascii_lowercase(), Vec::new())),
            None => match groups.last_mut() {
                Some((_, values)) => values.push(arg.clone()),
                None => return Err(ConfigError::UnexpectedArgument { value: arg.clone() }),
            },
        }
    }
    Ok(groups)
}

/// Parsed as a strict i64 first and narrowed after, so a value that is not a
/// number and a number that is not a port fail with different errors.
fn port(flag: &str, values: &[String]) -> Result<u16, ConfigError> {
    let [value] = values else {
        return Err(ConfigError::WrongNumberOfArguments {
            flag: flag.to_string(),
        });
    };
    let n = strict_i64(value.as_bytes()).ok_or_else(|| ConfigError::NotAnInteger {
        flag: flag.to_string(),
        value: value.clone(),
    })?;
    u16::try_from(n).map_err(|_| ConfigError::OutOfRange {
        flag: flag.to_string(),
        value: value.clone(),
        min: 0,
        max: u16::MAX.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(String::from).collect()
    }

    fn port_err(value: &str) -> Result<Config, ConfigError> {
        parse(&["--port".to_string(), value.to_string()])
    }

    #[test]
    fn no_flags_gives_the_defaults() {
        assert_eq!(parse(&[]), Ok(Config::default()));
        assert_eq!(Config::default().net.port, 6379);
    }

    #[test]
    fn a_port_is_read() {
        assert_eq!(parse(&args("--port 7000")).unwrap().net.port, 7000);
    }

    #[test]
    fn flag_names_ignore_case() {
        assert_eq!(parse(&args("--PORT 7000")).unwrap().net.port, 7000);
    }

    #[test]
    fn the_last_port_wins() {
        assert_eq!(parse(&args("--port 1 --port 2")).unwrap().net.port, 2);
    }

    #[test]
    fn a_port_must_be_a_strict_integer() {
        for value in ["abc", "06395", "+7000", " 7000"] {
            assert_eq!(
                port_err(value),
                Err(ConfigError::NotAnInteger {
                    flag: "port".into(),
                    value: value.into()
                }),
                "{value:?}"
            );
        }
    }

    #[test]
    fn a_port_must_fit_in_sixteen_bits() {
        for value in ["-1", "70000"] {
            assert_eq!(
                port_err(value),
                Err(ConfigError::OutOfRange {
                    flag: "port".into(),
                    value: value.into(),
                    min: 0,
                    max: 65535
                }),
                "{value:?}"
            );
        }
    }

    #[test]
    fn a_port_takes_exactly_one_argument() {
        for argv in ["--port", "--port 7000 extra", "--port --other"] {
            assert_eq!(
                parse(&args(argv)),
                Err(ConfigError::WrongNumberOfArguments {
                    flag: "port".into()
                }),
                "{argv:?}"
            );
        }
    }

    #[test]
    fn port_zero_listens_nowhere() {
        assert_eq!(parse(&args("--port 0")), Err(ConfigError::NotListening));
        assert_eq!(
            parse(&args("--port 1 --port 0")),
            Err(ConfigError::NotListening),
            "the last one wins"
        );
    }

    #[test]
    fn an_unknown_flag_is_an_error() {
        assert_eq!(
            parse(&args("--nosuchflag 1")),
            Err(ConfigError::UnknownFlag {
                flag: "nosuchflag".into()
            })
        );
    }

    #[test]
    fn an_argument_before_any_flag_is_an_error() {
        assert_eq!(
            parse(&args("redis.conf --port 7000")),
            Err(ConfigError::UnexpectedArgument {
                value: "redis.conf".into()
            })
        );
    }

    #[test]
    fn errors_name_the_flag_and_the_value() {
        let e = port_err("abc").unwrap_err();
        assert_eq!(
            e.to_string(),
            "--port \"abc\": argument couldn't be parsed into an integer"
        );
    }
}
