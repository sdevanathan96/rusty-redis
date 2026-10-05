use bytes::Bytes;

use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Value;


pub(super) fn try_parse(
    upper: &[u8],
    rest: &[Bytes],
    name: &Bytes,
) -> Option<Result<Command, CommandError>> {
    Some(match upper {
        b"INCR" => incr_command(rest, name),
        _ => return None,          // not an integer command
    })
}

fn incr_command(rest: &[Bytes], name: &Bytes) -> Result<Command, CommandError>{
    match rest {
        [key] => Ok(Command::Incr { key: key.clone() }),
        _ => Err(CommandError::WrongArity(name.clone())),
    }
}

pub(super) fn incr(key: &Bytes, db: &mut Db) -> Result<Value, CommandError> {
    let value = db.incr(key.clone())?;
    Ok(Value::Integer(value))
}