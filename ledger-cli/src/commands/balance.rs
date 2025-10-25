use std::io::BufWriter;

use anyhow::Result;
use ledger_core::{
    filters::AccountFilter,
    journal::Journal,
    report::{BalanceReport, ReportError, ReportGenerator, ReportOptions, SortCriteria},
};

use crate::{cli::BalanceArgs, Session};

pub fn balance(session: &Session, args: &BalanceArgs) -> Result<i32> {
    let journal = &session.parsed_journal.as_ref().ok_or(anyhow::anyhow!("No journal loaded"))?;

    let mut writer = BufWriter::new(std::io::stdout());
    write_report(&mut writer, journal, args)?;

    Ok(0)
}

fn write_report(
    writer: &mut impl std::io::Write,
    journal: &Journal,
    args: &BalanceArgs,
) -> Result<(), ReportError> {
    let report = BalanceReport::new(journal.clone());

    let mut report = if !args.pattern.is_empty() {
        let pattern = args.pattern.join("*.|.*");
        let pattern = format!(".*(?i:{pattern}).*");

        report.with_posting_filter(Box::new(
            AccountFilter::new(pattern.as_str()).expect("creating filter with pattern"),
        ))
    } else {
        report
    };

    let BalanceArgs { empty, flat, depth, collapse, no_total, .. } = *args;
    let options = ReportOptions {
        flat,
        depth,
        empty,
        sort_by: SortCriteria::Name,
        collapse,
        final_total: !no_total,
        running_total: false,
        account_width: None,
        amount_width: None,
        show_codes: false,
        currency: None,
    };

    report.generate(writer, &options)
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;
    use ledger_core::parser::JournalParser;

    use super::*;

    fn default_args() -> BalanceArgs {
        BalanceArgs {
            pattern: vec![],
            empty: false,
            flat: false,
            depth: Some(3),
            no_total: false,
            percent: false,
            invert: false,
            collapse: false,
        }
    }

    fn get_journal(input: Option<String>) -> Journal {
        let input = input.unwrap_or(textwrap::dedent(
            "
            2012-01-01 * Opening balances
                Assets:A                      10.00
                Equity:Opening balances      -10.00

            2012-01-02 * A to B
                Assets:A                     -10.00
                Assets:B                      10.00

            2012-01-03 * B partly to C
                Assets:B                      -5.00
                Assets:C                       5.00

            2012-01-04 * Borrow
                Assets:A                      10.00
                Liabilities:A                -10.00

            2012-01-05 * Return A
                Assets:A                     -10.00
                Liabilities:A                 10.00
            ",
        ));
        let mut parser = JournalParser::new();
        parser.parse_journal(&input).unwrap()
    }

    fn get_report(input: Option<String>, args: Option<BalanceArgs>) -> String {
        let mut buffer = Vec::new();
        write_report(&mut buffer, &get_journal(input), &args.unwrap_or(default_args()))
            .expect("writing to string");
        String::from_utf8(buffer).unwrap()
    }

    #[test]
    fn test_balance_report() {
        assert_snapshot!(
            get_report(None, None),
            @r#"
                                  10  Assets
                                   5    B
                                   5    C
                                 -10  Equity:Opening balances
                --------------------
                                   0
            "#
        );
    }

    #[test]
    fn test_balance_report_empty() {
        let mut args_with_empty = default_args();
        args_with_empty.empty = true;

        assert_snapshot!(
            get_report(None, Some(args_with_empty)),
            @r#"
                                  10  Assets
                                   0    A
                                   5    B
                                   5    C
                                 -10  Equity:Opening balances
                                   0  Liabilities:A
                --------------------
                                   0
            "#
        );
    }

    #[test]
    fn test_balance_report_collapse() {
        let mut args_with_empty = default_args();
        args_with_empty.collapse = true;

        assert_snapshot!(
            get_report(None, Some(args_with_empty)),
            @r#"
                                  10  Assets
                                 -10  Equity
                --------------------
                                   0
            "#
        );
    }

    #[test]
    fn test_balance_report_empty_parent_has_transactions() {
        let input = textwrap::dedent(
            "
            2021/01/01 Test
               	Left Column:Account A:Sub-Account    100
               	Right Column:Account 1:Sub-Account  -100

            2021/01/02 Test
               	Left Column:Account A               -100
               	Right Column:Account 2               100
            ",
        );

        assert_snapshot!(
            get_report(Some(input), None),
            @r#"
                                   0  Left Column:Account A
                                 100    Sub-Account
                                   0  Right Column
                                -100    Account 1:Sub-Account
                                 100    Account 2
                --------------------
                                   0
            "#
        );
    }
}
