use crate::{command::{Command, CommandError, Outcome}, db::{Db, EntryId, IdSpec}, resp::Value};

fn parse_xadd_id(raw: &[u8]) -> Result<IdSpec, CommandError> {
    if raw == b"*" {
        return Ok(IdSpec::Auto);
    }

    let text = std::str::from_utf8(raw).map_err(|_| CommandError::InvalidStreamId)?;

    match text.split_once('-') {
        None => {
            // bare millisecond: sequence is zero
            let ms = text.parse().map_err(|_| CommandError::InvalidStreamId)?;
            Ok(IdSpec::Explicit(EntryId { ms, seq: 0 }))
        }
        Some((ms, "*")) => {
            let ms = ms.parse().map_err(|_| CommandError::InvalidStreamId)?;
            Ok(IdSpec::AutoSeq(ms))
        }
        Some((ms, seq)) => Ok(IdSpec::Explicit(EntryId {
            ms: ms.parse().map_err(|_| CommandError::InvalidStreamId)?,
            seq: seq.parse().map_err(|_| CommandError::InvalidStreamId)?,
        })),
    }
}

/// Field/value pairs from a flat argument list. At least one pair, and the
/// count must be even.
fn parse_fields(
    args: &[Vec<u8>],
    name: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CommandError> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(CommandError::WrongArity(name.to_vec()));
    }
    Ok(args
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect())
}

pub(super) fn try_parse(
    upper: &[u8],
    rest: &[Vec<u8>],
    name: &[u8],
) -> Option<Result<Command, CommandError>> {
    Some(match upper {
        b"XADD" => xadd_command(rest, name),
        _ => return None,          // not a list command
    })
}



fn xadd_command(rest: &[Vec<u8>], name: &[u8]) -> Result<Command, CommandError> {
    match rest {
        [key, id, fields @ ..] => Ok(Command::XAdd {
            key: key.clone(),
            id: parse_xadd_id(id)?,
            fields: parse_fields(fields, name)?,
        }),
        _ => return Err(CommandError::WrongArity(name.to_vec())),
    }
}

pub(super) fn xadd(key: &Vec<u8>, id: IdSpec, fields: Vec<(Vec<u8>, Vec<u8>)>, db: &mut Db) -> Result<Outcome, CommandError> {
    Ok(Outcome::Reply(Value::BulkString(db.xadd(key, id, fields)?.to_bytes())))
}