use std::path::Path;
use std::process::ExitCode;

use journal_storage_sqlite::{Database, RecoveryAudit};

fn run(arguments: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.len() < 3 {
        return Err("usage: journal-recover COMMAND DATABASE AUDIT [PATH ...]; commands: close, backup DESTINATION, restore BACKUP DESTINATION APPROVAL --adapters-quiesced, reconcile APPROVAL --adapters-quiesced, reopen APPROVAL".into());
    }
    let database = Database::open_existing(&arguments[1])?;
    let audit = RecoveryAudit::open(&database, Path::new(&arguments[2]))?;
    match (arguments[0].as_str(), &arguments[3..]) {
        ("close", []) => audit.close()?,
        ("backup", [destination]) => {
            audit.backup(&database, Path::new(destination))?;
        }
        ("restore", [backup, destination, approval, quiesced])
            if quiesced == "--adapters-quiesced" =>
        {
            let evidence = audit.restore(Path::new(backup), Path::new(destination), true)?;
            RecoveryAudit::write_approval(Path::new(approval), &evidence)?;
        }
        ("reconcile", [approval, quiesced]) if quiesced == "--adapters-quiesced" => {
            let evidence = audit.reconcile(&database, true)?;
            RecoveryAudit::write_approval(Path::new(approval), &evidence)?;
        }
        ("reopen", [approval]) => {
            audit.reopen(
                &database,
                &RecoveryAudit::read_approval(Path::new(approval))?,
            )?;
        }
        _ => return Err("invalid recovery command or argument count".into()),
    }
    Ok(())
}

fn main() -> ExitCode {
    match run(&std::env::args().skip(1).collect::<Vec<_>>()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("journal-recover: {error}");
            ExitCode::FAILURE
        }
    }
}
