//! Scope implementations for core ledger types
//!
//! Implements the `Scope` trait for `Posting` and `Transaction`, allowing
//! expressions to be evaluated directly against these types.

use super::scope::Scope;
use super::Value;
use crate::posting::{Posting, PostingStatus};
use crate::transaction::{Transaction, TransactionStatus};

/// Scope wrapper for a `Transaction`.
///
/// Provides lookup for transaction-level fields: date, payee, note, code,
/// status flags, posting count, and metadata tags.
pub struct TransactionScope<'a> {
    transaction: &'a Transaction,
    parent: Option<&'a dyn Scope>,
}

impl<'a> TransactionScope<'a> {
    /// Create a new transaction scope.
    pub fn new(transaction: &'a Transaction, parent: Option<&'a dyn Scope>) -> Self {
        Self { transaction, parent }
    }
}

impl Scope for TransactionScope<'_> {
    fn lookup(&self, name: &str) -> Option<Value> {
        let txn = self.transaction;
        match name {
            "date" => Some(Value::Date(txn.date)),
            "aux_date" | "effective_date" => txn.aux_date.map(Value::Date),
            "payee" => Some(Value::String(txn.payee.clone())),
            "note" => txn.note.as_ref().map(|n| Value::String(n.clone())),
            "code" => txn.code.as_ref().map(|c| Value::String(c.clone())),
            "cleared" => {
                Some(Value::Bool(matches!(txn.status, TransactionStatus::Cleared)))
            }
            "pending" => {
                Some(Value::Bool(matches!(txn.status, TransactionStatus::Pending)))
            }
            "uncleared" => {
                Some(Value::Bool(matches!(txn.status, TransactionStatus::Uncleared)))
            }
            "posting_count" => Some(Value::Integer(txn.postings.len() as i64)),
            _ if name.starts_with("tag_") => {
                let tag_name = &name[4..];
                txn.metadata.get(tag_name).map(|td| {
                    if let Some(ref val) = td.value {
                        Value::String(val.clone())
                    } else {
                        Value::Bool(true)
                    }
                })
            }
            _ => None,
        }
    }

    fn parent(&self) -> Option<&dyn Scope> {
        self.parent.map(|p| p as &dyn Scope)
    }

    fn description(&self) -> &str {
        "transaction_scope"
    }
}

/// Scope wrapper for a `Posting`.
///
/// Provides lookup for posting-level fields: account, amount, cost,
/// status, virtual flag, payee override, note, and metadata tags.
/// Unknown names are delegated to the parent (typically a `TransactionScope`).
pub struct PostingScope<'a> {
    posting: &'a Posting,
    transaction: Option<&'a Transaction>,
    parent: Option<&'a dyn Scope>,
}

impl<'a> PostingScope<'a> {
    /// Create a new posting scope.
    ///
    /// `transaction` is provided so that the posting can access
    /// transaction-level payee as a fallback. The `parent` is typically
    /// a `TransactionScope` for full chain resolution.
    pub fn new(
        posting: &'a Posting,
        transaction: Option<&'a Transaction>,
        parent: Option<&'a dyn Scope>,
    ) -> Self {
        Self { posting, transaction, parent }
    }
}

impl Scope for PostingScope<'_> {
    fn lookup(&self, name: &str) -> Option<Value> {
        let post = self.posting;
        match name {
            "account" | "account_name" => {
                Some(Value::String(post.account.borrow().fullname_immutable()))
            }
            "amount" => post.amount.as_ref().map(|a| Value::Amount(a.clone())),
            "cost" => post.cost.as_ref().map(|c| Value::Amount(c.clone())),
            "virtual" => Some(Value::Bool(post.is_virtual())),
            "status" => {
                let s = match post.status {
                    PostingStatus::Uncleared => "uncleared",
                    PostingStatus::Cleared => "cleared",
                    PostingStatus::Pending => "pending",
                };
                Some(Value::String(s.to_string()))
            }
            "payee" => {
                // Posting-level payee override, or fall back to transaction payee
                if let Some(ref payee) = post.payee {
                    Some(Value::String(payee.to_string()))
                } else {
                    self.transaction.map(|t| Value::String(t.payee.clone()))
                }
            }
            "note" => post.note.as_ref().map(|n| Value::String(n.to_string())),
            _ if name.starts_with("tag_") => {
                let tag_name = &name[4..];
                post.metadata.get(tag_name).map(|td| {
                    if let Some(ref val) = td.value {
                        Value::String(val.clone())
                    } else {
                        Value::Bool(true)
                    }
                })
            }
            _ => None,
        }
    }

    fn parent(&self) -> Option<&dyn Scope> {
        self.parent.map(|p| p as &dyn Scope)
    }

    fn description(&self) -> &str {
        "posting_scope"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::AccountTree;
    use crate::expr::scope::resolve;
    use crate::transaction::{TransactionBuilder, TransactionStatus};
    use chrono::NaiveDate;
    use ledger_math::{commodity::Commodity, Amount, Decimal};
    use std::sync::Arc;

    fn usd_commodity() -> Option<Arc<Commodity>> {
        Some(Arc::new(Commodity::new("USD")))
    }

    fn make_test_data() -> (Transaction, AccountTree) {
        let mut tree = AccountTree::new();
        let checking = tree.find_account("Assets:Checking", true).unwrap();
        let groceries = tree.find_account("Expenses:Groceries", true).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();

        let txn = TransactionBuilder::new(date, "Whole Foods".to_string())
            .code("CHK100")
            .note("Weekly groceries")
            .status(TransactionStatus::Cleared)
            .aux_date(NaiveDate::from_ymd_opt(2024, 3, 16).unwrap())
            .tag("category", Some("food"))
            .tag("verified", None::<String>)
            .post_to(
                checking,
                Amount::with_commodity(Decimal::new(-4250, 2), usd_commodity()),
            )
            .post_to(
                groceries,
                Amount::with_commodity(Decimal::new(4250, 2), usd_commodity()),
            )
            .build()
            .unwrap();

        (txn, tree)
    }

    // --- TransactionScope tests ---

    #[test]
    fn test_transaction_scope_date() {
        let (txn, _tree) = make_test_data();
        let scope = TransactionScope::new(&txn, None);
        assert_eq!(
            scope.lookup("date"),
            Some(Value::Date(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()))
        );
    }

    #[test]
    fn test_transaction_scope_aux_date() {
        let (txn, _tree) = make_test_data();
        let scope = TransactionScope::new(&txn, None);
        assert_eq!(
            scope.lookup("aux_date"),
            Some(Value::Date(NaiveDate::from_ymd_opt(2024, 3, 16).unwrap()))
        );
        assert_eq!(
            scope.lookup("effective_date"),
            Some(Value::Date(NaiveDate::from_ymd_opt(2024, 3, 16).unwrap()))
        );
    }

    #[test]
    fn test_transaction_scope_payee() {
        let (txn, _tree) = make_test_data();
        let scope = TransactionScope::new(&txn, None);
        assert_eq!(scope.lookup("payee"), Some(Value::String("Whole Foods".into())));
    }

    #[test]
    fn test_transaction_scope_note() {
        let (txn, _tree) = make_test_data();
        let scope = TransactionScope::new(&txn, None);
        assert_eq!(scope.lookup("note"), Some(Value::String("Weekly groceries".into())));
    }

    #[test]
    fn test_transaction_scope_code() {
        let (txn, _tree) = make_test_data();
        let scope = TransactionScope::new(&txn, None);
        assert_eq!(scope.lookup("code"), Some(Value::String("CHK100".into())));
    }

    #[test]
    fn test_transaction_scope_status_flags() {
        let (txn, _tree) = make_test_data();
        let scope = TransactionScope::new(&txn, None);
        assert_eq!(scope.lookup("cleared"), Some(Value::Bool(true)));
        assert_eq!(scope.lookup("pending"), Some(Value::Bool(false)));
        assert_eq!(scope.lookup("uncleared"), Some(Value::Bool(false)));
    }

    #[test]
    fn test_transaction_scope_posting_count() {
        let (txn, _tree) = make_test_data();
        let scope = TransactionScope::new(&txn, None);
        assert_eq!(scope.lookup("posting_count"), Some(Value::Integer(2)));
    }

    #[test]
    fn test_transaction_scope_tags() {
        let (txn, _tree) = make_test_data();
        let scope = TransactionScope::new(&txn, None);
        assert_eq!(scope.lookup("tag_category"), Some(Value::String("food".into())));
        assert_eq!(scope.lookup("tag_verified"), Some(Value::Bool(true)));
        assert_eq!(scope.lookup("tag_missing"), None);
    }

    #[test]
    fn test_transaction_scope_unknown_returns_none() {
        let (txn, _tree) = make_test_data();
        let scope = TransactionScope::new(&txn, None);
        assert_eq!(scope.lookup("nonexistent"), None);
    }

    #[test]
    fn test_transaction_scope_no_aux_date() {
        let mut tree = AccountTree::new();
        let a = tree.find_account("A", true).unwrap();
        let b = tree.find_account("B", true).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let txn = TransactionBuilder::new(date, "Test".to_string())
            .post_to(a, Amount::with_commodity(Decimal::new(-100, 0), usd_commodity()))
            .post_to(b, Amount::with_commodity(Decimal::new(100, 0), usd_commodity()))
            .build()
            .unwrap();

        let scope = TransactionScope::new(&txn, None);
        assert_eq!(scope.lookup("aux_date"), None);
        assert_eq!(scope.lookup("note"), None);
        assert_eq!(scope.lookup("code"), None);
    }

    // --- PostingScope tests ---

    #[test]
    fn test_posting_scope_account_name() {
        let (txn, _tree) = make_test_data();
        let posting = &txn.postings[0];
        let scope = PostingScope::new(posting, Some(&txn), None);

        let account = scope.lookup("account");
        assert!(account.is_some());
        if let Some(Value::String(name)) = account {
            assert!(name.contains("Checking"));
        }

        assert_eq!(scope.lookup("account"), scope.lookup("account_name"));
    }

    #[test]
    fn test_posting_scope_amount() {
        let (txn, _tree) = make_test_data();
        let posting = &txn.postings[1]; // Expenses:Groceries +42.50
        let scope = PostingScope::new(posting, Some(&txn), None);

        let amount = scope.lookup("amount");
        assert!(amount.is_some());
        if let Some(Value::Amount(a)) = amount {
            assert_eq!(a.value(), Decimal::new(4250, 2));
        }
    }

    #[test]
    fn test_posting_scope_cost_none() {
        let (txn, _tree) = make_test_data();
        let posting = &txn.postings[0];
        let scope = PostingScope::new(posting, Some(&txn), None);

        assert_eq!(scope.lookup("cost"), None);
    }

    #[test]
    fn test_posting_scope_virtual_flag() {
        let (txn, _tree) = make_test_data();
        let posting = &txn.postings[0];
        let scope = PostingScope::new(posting, Some(&txn), None);

        assert_eq!(scope.lookup("virtual"), Some(Value::Bool(false)));
    }

    #[test]
    fn test_posting_scope_status() {
        let (txn, _tree) = make_test_data();
        let posting = &txn.postings[0];
        let scope = PostingScope::new(posting, Some(&txn), None);

        assert_eq!(scope.lookup("status"), Some(Value::String("uncleared".into())));
    }

    #[test]
    fn test_posting_scope_payee_fallback_to_transaction() {
        let (txn, _tree) = make_test_data();
        let posting = &txn.postings[0]; // no posting-level payee
        let scope = PostingScope::new(posting, Some(&txn), None);

        assert_eq!(scope.lookup("payee"), Some(Value::String("Whole Foods".into())));
    }

    #[test]
    fn test_posting_scope_note_none() {
        let (txn, _tree) = make_test_data();
        let posting = &txn.postings[0];
        let scope = PostingScope::new(posting, Some(&txn), None);

        assert_eq!(scope.lookup("note"), None);
    }

    #[test]
    fn test_posting_scope_metadata_tags() {
        let (txn, _tree) = make_test_data();
        // The test postings don't have posting-level metadata, so these should be None
        let posting = &txn.postings[0];
        let scope = PostingScope::new(posting, Some(&txn), None);

        assert_eq!(scope.lookup("tag_something"), None);
    }

    #[test]
    fn test_posting_with_metadata() {
        let mut tree = AccountTree::new();
        let a = tree.find_account("A", true).unwrap();
        let b = tree.find_account("B", true).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let mut txn = TransactionBuilder::new(date, "Test".to_string())
            .post_to(a, Amount::with_commodity(Decimal::new(-100, 0), usd_commodity()))
            .post_to(b, Amount::with_commodity(Decimal::new(100, 0), usd_commodity()))
            .build()
            .unwrap();

        txn.postings[0].set_tag("receipt".to_string(), Some("abc123".to_string()), false);
        txn.postings[0].set_tag("reviewed".to_string(), None, false);

        let scope = PostingScope::new(&txn.postings[0], Some(&txn), None);
        assert_eq!(scope.lookup("tag_receipt"), Some(Value::String("abc123".into())));
        assert_eq!(scope.lookup("tag_reviewed"), Some(Value::Bool(true)));
    }

    // --- Scope chain (PostingScope -> TransactionScope) ---

    #[test]
    fn test_posting_to_transaction_chain_resolve() {
        let (txn, _tree) = make_test_data();
        let txn_scope = TransactionScope::new(&txn, None);
        let post_scope = PostingScope::new(&txn.postings[0], Some(&txn), Some(&txn_scope));

        // Posting-level fields
        let account = resolve(&post_scope, "account");
        assert!(account.is_some());

        // Transaction-level fields via chain
        assert_eq!(resolve(&post_scope, "date"), Some(Value::Date(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap())));
        assert_eq!(resolve(&post_scope, "cleared"), Some(Value::Bool(true)));
        assert_eq!(resolve(&post_scope, "tag_category"), Some(Value::String("food".into())));

        // Missing in both
        assert_eq!(resolve(&post_scope, "nonexistent"), None);
    }

    #[test]
    fn test_description() {
        let (txn, _tree) = make_test_data();
        let txn_scope = TransactionScope::new(&txn, None);
        let post_scope = PostingScope::new(&txn.postings[0], Some(&txn), None);

        assert_eq!(txn_scope.description(), "transaction_scope");
        assert_eq!(post_scope.description(), "posting_scope");
    }
}
