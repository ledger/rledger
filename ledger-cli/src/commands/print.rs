use std::io::BufWriter;

use anyhow::Result;
use ledger_core::{
    filters::AccountFilter,
    journal::Journal,
    report::{PrintReport, ReportError, ReportGenerator, ReportOptions},
};

use crate::{cli::PrintArgs, Session};

pub fn print(session: &Session, args: &PrintArgs) -> Result<i32> {
    let journal = &session.parsed_journal.as_ref().ok_or(anyhow::anyhow!("No journal loaded"))?;

    let mut writer = BufWriter::new(std::io::stdout());
    write_report(&mut writer, journal, args)?;

    if !args.pattern.is_empty() && session.verbose_enabled {
        eprintln!("Account patterns: {:?}", args.pattern);
    }

    if args.raw && session.verbose_enabled {
        eprintln!("Raw format option not yet implemented");
    }

    Ok(0)
}

fn write_report(
    writer: &mut impl std::io::Write,
    journal: &Journal,
    args: &PrintArgs,
) -> Result<(), ReportError> {
    let report = PrintReport::new(journal.clone());

    let mut report = if !args.pattern.is_empty() {
        let pattern = args.pattern.join("*.|.*");
        let pattern = format!(".*(?i:{pattern}).*");

        report.with_transaction_filter(Box::new(
            AccountFilter::new(pattern.as_str()).expect("creating filter with pattern"),
        ))
    } else {
        report
    };

    let options = ReportOptions::default();

    report.generate(writer, &options)
}
