use anyhow::Result;
use ledger_core::{
    filters::AccountFilter,
    journal::Journal,
    report::{RegisterReport, ReportError, ReportGenerator, ReportOptions},
};
use std::io::BufWriter;

use crate::{cli::RegisterArgs, Session};

pub fn register(session: &Session, args: &RegisterArgs) -> Result<i32> {
    let journal = &session.parsed_journal.as_ref().ok_or(anyhow::anyhow!("No journal loaded"))?;

    let mut writer = BufWriter::new(std::io::stdout());
    write_report(&mut writer, journal, args)?;

    if !args.pattern.is_empty() && session.verbose_enabled {
        eprintln!("Account patterns: {:?}", args.pattern);
    }

    if args.subtotal && session.verbose_enabled {
        eprintln!("Subtotal option not yet implemented");
    }

    Ok(0)
}

fn write_report(
    writer: &mut impl std::io::Write,
    journal: &Journal,
    args: &RegisterArgs,
) -> Result<(), ReportError> {
    let report = RegisterReport::new(journal.clone())
        .with_wide_format(args.wide)
        .with_show_related(args.related);

    let mut report = if !args.pattern.is_empty() {
        let pattern = args.pattern.join("*.|.*");
        let pattern = format!(".*(?i:{pattern}).*");

        report.with_posting_filter(Box::new(
            AccountFilter::new(pattern.as_str()).expect("creating filter with pattern"),
        ))
    } else {
        report
    };

    let options = ReportOptions::default();

    report.generate(writer, &options)
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;
    use ledger_core::parser::JournalParser;

    use super::*;

    fn default_args() -> RegisterArgs {
        RegisterArgs {
            pattern: vec![],
            subtotal: false,
            head: None,
            tail: None,
            related: false,
            wide: false,
        }
    }

    #[test]
    fn test_register_report() {
        let input = textwrap::dedent(
            "
            2012-01-10 * Phone expense on holidays
                Expenses:Phone            12.00 EUR @@ 10.00 GBP
                Assets:Cash              -10.00 GBP

            2012-01-31 * Rent expense
                Expenses:Rent            550.00 GBP
                Assets:Cash             -550.00 GBP

            2012-02-01 * Buy AAA
                Assets:Investment             1 AAA @ 10.00 GBP
                Assets:Cash              -10.00 GBP
            ",
        );
        let mut parser = JournalParser::new();
        let journal = parser.parse_journal(&input).unwrap();

        let report = {
            let mut buffer = Vec::new();
            write_report(&mut buffer, &journal, &default_args()).expect("writing to string");
            String::from_utf8(buffer).unwrap()
        };

        assert_snapshot!(report, @r#"
            12-Jan-10 Phone expense on ho.. Expenses:Phone            12.00 EUR    12.00 EUR
                                            Assets:Cash              -10.00 GBP    12.00 EUR
                                                                                  -10.00 GBP
            12-Jan-31 Rent expense          Expenses:Rent            550.00 GBP    12.00 EUR
                                                                                  540.00 GBP
                                            Assets:Cash             -550.00 GBP    12.00 EUR
                                                                                  -10.00 GBP
            12-Feb-01 Buy AAA               Assets:Investment             1 AAA        1 AAA
                                                                                   12.00 EUR
                                                                                  -10.00 GBP
                                            Assets:Cash              -10.00 GBP        1 AAA
                                                                                   12.00 EUR
                                                                                  -20.00 GBP
        "#);
    }
}
