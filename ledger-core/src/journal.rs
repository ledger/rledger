//! Journal data structure for storing transactions

use crate::account::{Account, AccountRef};
use crate::transaction::Transaction;
use ledger_math::commodity::Commodity;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

/// Main journal containing all transactions
#[derive(Debug, Default, Clone)]
pub struct Journal {
    /// List of transactions
    pub transactions: Vec<Transaction>,
    /// Account registry
    pub accounts: HashMap<String, AccountRef>,
    /// Commodity registry
    pub commodities: HashMap<String, Arc<Commodity>>,
    /// Next account ID
    next_account_id: usize,
}

impl Journal {
    /// Create a new empty journal
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a transaction to the journal
    pub fn add_transaction(&mut self, transaction: Transaction) {
        self.transactions.push(transaction);
    }

    /// Find an account by name
    pub fn find_account(&self, name: &str) -> Result<AccountRef, String> {
        self.accounts.get(name).cloned().ok_or_else(|| format!("Account not found: {}", name))
    }

    /// Get or create an account by name
    pub fn get_or_create_account(&mut self, name: &str) -> AccountRef {
        if let Some(account) = self.accounts.get(name) {
            account.clone()
        } else {
            let account_id = self.next_account_id;
            self.next_account_id += 1;
            let account = Rc::new(RefCell::new(Account::new(name.into(), None, account_id)));
            self.accounts.insert(name.to_string(), account.clone());
            account
        }
    }

    /// Add an account to the journal
    pub fn add_account(&mut self, account: AccountRef) -> Result<(), String> {
        let name = account.borrow().name().to_string();
        if self.accounts.contains_key(&name) {
            return Err(format!("Account already exists: {}", name));
        }
        self.accounts.insert(name, account);
        Ok(())
    }

    /// Add a commodity to the journal
    pub fn add_commodity(&mut self, commodity: Arc<Commodity>) -> Result<(), String> {
        let symbol = commodity.symbol().to_string();
        if self.commodities.contains_key(&symbol) {
            return Err(format!("Commodity already exists: {}", symbol));
        }
        self.commodities.insert(symbol, commodity);
        Ok(())
    }

    /// Merge another journal into this one
    pub fn merge(&mut self, other: Journal) -> Result<(), String> {
        // Merge transactions
        self.transactions.extend(other.transactions);

        // Merge accounts
        for (name, account) in other.accounts {
            self.accounts.entry(name).or_insert(account);
        }

        // Merge commodities
        for (symbol, commodity) in other.commodities {
            self.commodities.entry(symbol).or_insert(commodity);
        }

        Ok(())
    }

    /// Format all transactions and write them to the given writer.
    pub fn write_transactions(
        &self,
        writer: &mut impl std::io::Write,
    ) -> Result<(), crate::report::ReportError> {
        use crate::report::{PrintReport, ReportGenerator};
        PrintReport::new(self.clone()).generate(writer, &Default::default())
    }

    /// Format all transactions and return them as a String
    pub fn format_transactions(&self) -> String {
        let mut buffer = Vec::new();
        self.write_transactions(&mut buffer).expect("writing to string");
        String::from_utf8(buffer).unwrap()
    }
}

#[cfg(test)]
mod tests {

    use insta::assert_snapshot;

    use crate::parser::{reset_parse_state, JournalParser};

    #[test]
    fn test_parse_and_format_journal() {
        reset_parse_state();

        let input = textwrap::dedent(
            "
            1999/11/01 * Achat
                Actif:SSB      125 STK
                Actif:SSB            -1672,42 $

            1999/11/04 * Vente
                Actif:SSB        -125 STK
                Dépense:SSB:Commissions       55,07 $
                Actif:SSB             1821,54 $
            ",
        );
        let mut parser = JournalParser::new();
        let journal = parser.parse_journal(&input).unwrap();

        assert_snapshot!(journal.format_transactions(), @r#"
            1999/11/01 * Achat
                Actif:SSB                                125 STK
                Actif:SSB                             -1672,42 $

            1999/11/04 * Vente
                Actif:SSB                               -125 STK
                Dépense:SSB:Commissions                  55,07 $
                Actif:SSB                              1821,54 $
        "#);
    }

    #[test]
    fn test_parse_and_format_journal_optional_year() {
        reset_parse_state();

        let input = textwrap::dedent(
            "
            year 1999

            11/01 A
                B  $1
                C
            ",
        );
        let mut parser = JournalParser::new();
        let journal = parser.parse_journal(&input).unwrap();

        assert_snapshot!(journal.format_transactions(), @r#"
            1999/11/01 A
                B                                             $1
                C
        "#);
    }

    #[test]
    fn test_parse_and_format_journal_with_widening_commodities() {
        reset_parse_state();

        // from test/regress/1D275740.test
        let input = textwrap::dedent(
            "
            2001/12/21 Payee
                A                                             $1
                A                                         $1.000
                A                                             ¢1
                A
            ",
        );
        let mut parser = JournalParser::new();
        let journal = parser.parse_journal(&input).unwrap();

        // assert_debug_snapshot!(journal.transactions, @r#""#);
        assert_snapshot!(journal.format_transactions(), @r#"
            2001/12/21 Payee
                A                                         $1.000
                A                                         $1.000
                A                                             ¢1
                A
        "#);
    }

    #[test]
    fn test_parse_and_format_journal_with_default_commodity() {
        reset_parse_state();

        // from test/regress/1D275740.test
        let input = textwrap::dedent(
            "
            D 1.200,40 €

            2009/08/01 Achat
               Actif:SV                 1,0204 MFE @ 333,200 €
               Actif:BC                           -340,00 €

            2009/09/29 Vente
               Actif:SV                 -0,0415 MFE @ 358,800 €
               Actif:SV                      14,89 €
            ",
        );
        let mut parser = JournalParser::new();
        let journal = parser.parse_journal(&input).unwrap();

        // assert_debug_snapshot!(journal.transactions, @r#""#);
        assert_snapshot!(journal.format_transactions(), @r#"
            2009/08/01 Achat
                Actif:SV                              1,0204 MFE @ 333,20 €
                Actif:BC                               -340,00 €

            2009/09/29 Vente
                Actif:SV                              -0,0415 MFE @ 358,80 €
                Actif:SV                                 14,89 €
        "#);
    }
}
