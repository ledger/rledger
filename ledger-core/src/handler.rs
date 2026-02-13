//! PostHandler processing pipeline for posting transformation and reporting.
//!
//! This module implements the Chain of Responsibility pattern inspired by
//! C++ ledger's `post_handler`. Each handler receives a posting with its
//! parent transaction, processes it, and optionally forwards it to the
//! next handler in the chain.
//!
//! Unlike the predicate-only [`Filter<T>`](crate::filters::Filter) trait,
//! handlers can transform, accumulate, sort, and generate data.

use std::collections::HashMap;

use ledger_math::amount::Amount;

use crate::posting::Posting;
use crate::transaction::Transaction;

/// A handler in the posting processing pipeline.
///
/// Handlers form a chain where each handler receives postings, processes
/// them, and optionally passes them to the next handler. This enables
/// composable report generation pipelines.
///
/// # Examples
///
/// ```rust,ignore
/// let pipeline = PipelineBuilder::new(Box::new(CollectHandler::new()))
///     .filter(|p, _t| p.amount.is_some())
///     .sort(|p, _t| p.account_name())
///     .build();
/// ```
pub trait PostHandler {
    /// Process a posting with its parent transaction.
    fn handle(&mut self, posting: &Posting, transaction: &Transaction);

    /// Signal end of input. Buffering handlers should flush their
    /// accumulated state to the next handler.
    fn flush(&mut self);

    /// Description for debugging.
    fn description(&self) -> &str;
}

// ---------------------------------------------------------------------------
// CollectHandler — terminal handler that collects postings
// ---------------------------------------------------------------------------

/// Terminal handler that collects (posting, transaction) pairs into a `Vec`.
///
/// Useful as the final handler in a pipeline when you need to inspect
/// the results programmatically.
pub struct CollectHandler {
    collected: Vec<(Posting, Transaction)>,
}

impl CollectHandler {
    pub fn new() -> Self {
        Self { collected: Vec::new() }
    }

    /// Return a reference to the collected posting/transaction pairs.
    pub fn collected(&self) -> &[(Posting, Transaction)] {
        &self.collected
    }

    /// Consume the handler and return the collected pairs.
    pub fn into_collected(self) -> Vec<(Posting, Transaction)> {
        self.collected
    }
}

impl Default for CollectHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl PostHandler for CollectHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        self.collected.push((posting.clone(), transaction.clone()));
    }

    fn flush(&mut self) {
        // Nothing to flush — data is already stored.
    }

    fn description(&self) -> &str {
        "CollectHandler"
    }
}

// ---------------------------------------------------------------------------
// CountHandler — terminal handler that counts postings
// ---------------------------------------------------------------------------

/// Terminal handler that counts the number of postings it receives.
pub struct CountHandler {
    count: usize,
}

impl CountHandler {
    pub fn new() -> Self {
        Self { count: 0 }
    }

    /// Return the number of postings received so far.
    pub fn count(&self) -> usize {
        self.count
    }
}

impl Default for CountHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl PostHandler for CountHandler {
    fn handle(&mut self, _posting: &Posting, _transaction: &Transaction) {
        self.count += 1;
    }

    fn flush(&mut self) {}

    fn description(&self) -> &str {
        "CountHandler"
    }
}

// ---------------------------------------------------------------------------
// FilterHandler — conditionally forwards postings
// ---------------------------------------------------------------------------

/// Handler that forwards postings to the next handler only when a
/// predicate returns `true`.
pub struct FilterHandler {
    predicate: Box<dyn Fn(&Posting, &Transaction) -> bool>,
    next: Box<dyn PostHandler>,
}

impl FilterHandler {
    pub fn new(
        predicate: impl Fn(&Posting, &Transaction) -> bool + 'static,
        next: Box<dyn PostHandler>,
    ) -> Self {
        Self { predicate: Box::new(predicate), next }
    }
}

impl PostHandler for FilterHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        if (self.predicate)(posting, transaction) {
            self.next.handle(posting, transaction);
        }
    }

    fn flush(&mut self) {
        self.next.flush();
    }

    fn description(&self) -> &str {
        "FilterHandler"
    }
}

// ---------------------------------------------------------------------------
// CalcHandler — running total calculation
// ---------------------------------------------------------------------------

/// Handler that maintains a running total of posting amounts.
///
/// For each posting, the handler adds the posting's amount to the running
/// total, stores the total in the posting's extended data, and forwards
/// the posting to the next handler.
pub struct CalcHandler {
    running_total: Amount,
    count: usize,
    next: Box<dyn PostHandler>,
}

impl CalcHandler {
    pub fn new(next: Box<dyn PostHandler>) -> Self {
        Self { running_total: Amount::null(), count: 0, next }
    }

    /// Return the current running total.
    pub fn running_total(&self) -> &Amount {
        &self.running_total
    }

    /// Return the number of postings processed.
    pub fn count(&self) -> usize {
        self.count
    }
}

impl PostHandler for CalcHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        if let Some(ref amount) = posting.amount {
            // Accumulate into running total (ignore errors from commodity mismatch
            // in this simplified version — a production implementation would handle
            // multi-commodity totals via Balance).
            let _ = self.running_total.add_amount(amount);
        }
        self.count += 1;

        // Store running total in xdata on a clone and forward.
        let mut posting_clone = posting.clone();
        posting_clone.ensure_xdata();
        if let Some(ref mut xdata) = posting_clone.xdata {
            xdata.total = Some(self.running_total.clone());
            xdata.count = self.count;
        }
        self.next.handle(&posting_clone, transaction);
    }

    fn flush(&mut self) {
        self.next.flush();
    }

    fn description(&self) -> &str {
        "CalcHandler"
    }
}

// ---------------------------------------------------------------------------
// SortHandler — buffers all postings, sorts on flush
// ---------------------------------------------------------------------------

/// Handler that buffers all incoming postings and, on [`flush`](PostHandler::flush),
/// sorts them by a caller-supplied key before forwarding to the next handler.
pub struct SortHandler {
    buffer: Vec<(Posting, Transaction)>,
    sort_key: Box<dyn Fn(&Posting, &Transaction) -> String>,
    reverse: bool,
    next: Box<dyn PostHandler>,
}

impl SortHandler {
    pub fn new(
        sort_key: impl Fn(&Posting, &Transaction) -> String + 'static,
        reverse: bool,
        next: Box<dyn PostHandler>,
    ) -> Self {
        Self { buffer: Vec::new(), sort_key: Box::new(sort_key), reverse, next }
    }
}

impl PostHandler for SortHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        self.buffer.push((posting.clone(), transaction.clone()));
    }

    fn flush(&mut self) {
        let sort_key = &self.sort_key;
        self.buffer.sort_by(|(pa, ta), (pb, tb)| {
            let ka = sort_key(pa, ta);
            let kb = sort_key(pb, tb);
            ka.cmp(&kb)
        });
        if self.reverse {
            self.buffer.reverse();
        }
        for (posting, transaction) in &self.buffer {
            self.next.handle(posting, transaction);
        }
        self.buffer.clear();
        self.next.flush();
    }

    fn description(&self) -> &str {
        "SortHandler"
    }
}

// ---------------------------------------------------------------------------
// CollapseHandler — consolidates postings by account
// ---------------------------------------------------------------------------

/// Handler that consolidates postings sharing the same account name.
///
/// Amounts for postings to the same account are merged. On
/// [`flush`](PostHandler::flush), one consolidated posting per account is
/// forwarded to the next handler.
pub struct CollapseHandler {
    /// Map from account name → (accumulated amount, template transaction).
    /// The template posting is kept so we can reconstruct a posting for
    /// forwarding.
    accounts: HashMap<String, (Amount, Posting, Transaction)>,
    next: Box<dyn PostHandler>,
}

impl CollapseHandler {
    pub fn new(next: Box<dyn PostHandler>) -> Self {
        Self { accounts: HashMap::new(), next }
    }
}

impl PostHandler for CollapseHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        let account_name = posting.account_name();

        if let Some((ref mut total, _template_posting, _template_txn)) =
            self.accounts.get_mut(&account_name)
        {
            if let Some(ref amount) = posting.amount {
                let _ = total.add_amount(amount);
            }
        } else {
            let initial = posting.amount.clone().unwrap_or_else(Amount::null);
            self.accounts
                .insert(account_name, (initial, posting.clone(), transaction.clone()));
        }
    }

    fn flush(&mut self) {
        for (_name, (total, mut posting, transaction)) in self.accounts.drain() {
            posting.amount = Some(total);
            self.next.handle(&posting, &transaction);
        }
        self.next.flush();
    }

    fn description(&self) -> &str {
        "CollapseHandler"
    }
}

// ---------------------------------------------------------------------------
// PipelineBuilder — ergonomic pipeline construction
// ---------------------------------------------------------------------------

/// Builder for composing a chain of [`PostHandler`]s.
///
/// The builder wraps handlers inside-out: the terminal handler is provided
/// at construction, and each subsequent call wraps the current chain with
/// a new outer handler. Calling [`build`](PipelineBuilder::build) returns
/// the outermost handler.
///
/// # Examples
///
/// ```rust,ignore
/// let pipeline = PipelineBuilder::new(Box::new(CollectHandler::new()))
///     .filter(|p, _| p.amount.is_some())
///     .sort(|p, _| p.account_name())
///     .calc()
///     .build();
/// ```
pub struct PipelineBuilder {
    current: Box<dyn PostHandler>,
}

impl PipelineBuilder {
    /// Create a new builder with the given terminal handler.
    pub fn new(terminal: Box<dyn PostHandler>) -> Self {
        Self { current: terminal }
    }

    /// Wrap the current pipeline with a [`FilterHandler`].
    pub fn filter(
        self,
        predicate: impl Fn(&Posting, &Transaction) -> bool + 'static,
    ) -> Self {
        Self { current: Box::new(FilterHandler::new(predicate, self.current)) }
    }

    /// Wrap the current pipeline with a [`SortHandler`].
    pub fn sort(self, key: impl Fn(&Posting, &Transaction) -> String + 'static) -> Self {
        Self {
            current: Box::new(SortHandler::new(key, false, self.current)),
        }
    }

    /// Wrap the current pipeline with a reverse [`SortHandler`].
    pub fn sort_reverse(
        self,
        key: impl Fn(&Posting, &Transaction) -> String + 'static,
    ) -> Self {
        Self {
            current: Box::new(SortHandler::new(key, true, self.current)),
        }
    }

    /// Wrap the current pipeline with a [`CollapseHandler`].
    pub fn collapse(self) -> Self {
        Self { current: Box::new(CollapseHandler::new(self.current)) }
    }

    /// Wrap the current pipeline with a [`CalcHandler`].
    pub fn calc(self) -> Self {
        Self { current: Box::new(CalcHandler::new(self.current)) }
    }

    /// Consume the builder and return the composed pipeline.
    pub fn build(self) -> Box<dyn PostHandler> {
        self.current
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::AccountTree;
    use chrono::NaiveDate;
    use rust_decimal::Decimal;

    /// Helper: create a posting to the given account path with the given amount.
    fn make_posting(tree: &mut AccountTree, account: &str, value: i64) -> Posting {
        let acct = tree.find_account(account, true).unwrap();
        Posting::with_amount(acct, Amount::new(Decimal::from(value)))
    }

    /// Helper: create a minimal transaction on the given date with the given payee.
    fn make_transaction(date: (i32, u32, u32), payee: &str) -> Transaction {
        Transaction::new(
            NaiveDate::from_ymd_opt(date.0, date.1, date.2).unwrap(),
            payee.to_string(),
        )
    }

    // -- CollectHandler -----------------------------------------------------

    #[test]
    fn collect_handler_collects_postings() {
        let mut tree = AccountTree::new();
        let p1 = make_posting(&mut tree, "Expenses:Food", 50);
        let p2 = make_posting(&mut tree, "Assets:Cash", -50);
        let txn = make_transaction((2024, 1, 15), "Grocery");

        let mut handler = CollectHandler::new();
        handler.handle(&p1, &txn);
        handler.handle(&p2, &txn);
        handler.flush();

        assert_eq!(handler.collected().len(), 2);
        assert_eq!(handler.collected()[0].0.account_name(), "Expenses:Food");
        assert_eq!(handler.collected()[1].0.account_name(), "Assets:Cash");
    }

    // -- CountHandler -------------------------------------------------------

    #[test]
    fn count_handler_counts_postings() {
        let mut tree = AccountTree::new();
        let p1 = make_posting(&mut tree, "Expenses:Food", 50);
        let p2 = make_posting(&mut tree, "Assets:Cash", -50);
        let txn = make_transaction((2024, 1, 15), "Grocery");

        let mut handler = CountHandler::new();
        handler.handle(&p1, &txn);
        handler.handle(&p2, &txn);

        assert_eq!(handler.count(), 2);
    }

    // -- FilterHandler ------------------------------------------------------

    #[test]
    fn filter_handler_passes_matching_postings() {
        let mut tree = AccountTree::new();
        let p_positive = make_posting(&mut tree, "Expenses:Food", 50);
        let p_negative = make_posting(&mut tree, "Assets:Cash", -50);
        let txn = make_transaction((2024, 1, 15), "Grocery");

        let collector = Box::new(CollectHandler::new());
        // Only pass postings with positive amounts.
        let mut pipeline = FilterHandler::new(
            |p, _t| {
                p.amount
                    .as_ref()
                    .map(|a| a.value() > Decimal::ZERO)
                    .unwrap_or(false)
            },
            collector,
        );

        pipeline.handle(&p_positive, &txn);
        pipeline.handle(&p_negative, &txn);
        pipeline.flush();

        // Downcast not possible with trait objects directly; use CollectHandler
        // through a pipeline builder instead. We test via a full pipeline below.
    }

    #[test]
    fn filter_handler_via_pipeline() {
        let mut tree = AccountTree::new();
        let p_positive = make_posting(&mut tree, "Expenses:Food", 50);
        let p_negative = make_posting(&mut tree, "Assets:Cash", -50);
        let p_zero = make_posting(&mut tree, "Equity:Opening", 0);
        let txn = make_transaction((2024, 1, 15), "Grocery");

        let collect = CollectHandler::new();
        let collect_ptr = &collect as *const CollectHandler;

        // We need a way to inspect the terminal handler after the pipeline
        // runs. Use a shared-state wrapper via interior mutability.
        use std::cell::RefCell;
        use std::rc::Rc;

        let results: Rc<RefCell<Vec<(Posting, Transaction)>>> =
            Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);

        // Custom terminal that pushes to a shared vec.
        struct SharedCollector {
            results: Rc<RefCell<Vec<(Posting, Transaction)>>>,
        }
        impl PostHandler for SharedCollector {
            fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
                self.results.borrow_mut().push((posting.clone(), transaction.clone()));
            }
            fn flush(&mut self) {}
            fn description(&self) -> &str {
                "SharedCollector"
            }
        }

        let terminal = SharedCollector { results: results_clone };
        let mut pipeline = PipelineBuilder::new(Box::new(terminal))
            .filter(|p, _| {
                p.amount
                    .as_ref()
                    .map(|a| a.value() > Decimal::ZERO)
                    .unwrap_or(false)
            })
            .build();

        pipeline.handle(&p_positive, &txn);
        pipeline.handle(&p_negative, &txn);
        pipeline.handle(&p_zero, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0.account_name(), "Expenses:Food");

        // Avoid unused variable warning.
        let _ = collect_ptr;
    }

    // -- CalcHandler --------------------------------------------------------

    #[test]
    fn calc_handler_running_total() {
        let mut tree = AccountTree::new();
        let p1 = make_posting(&mut tree, "Expenses:Food", 50);
        let p2 = make_posting(&mut tree, "Expenses:Rent", 100);
        let p3 = make_posting(&mut tree, "Expenses:Transport", 25);
        let txn = make_transaction((2024, 1, 15), "Various");

        use std::cell::RefCell;
        use std::rc::Rc;

        let results: Rc<RefCell<Vec<(Posting, Transaction)>>> =
            Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);

        struct SharedCollector {
            results: Rc<RefCell<Vec<(Posting, Transaction)>>>,
        }
        impl PostHandler for SharedCollector {
            fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
                self.results.borrow_mut().push((posting.clone(), transaction.clone()));
            }
            fn flush(&mut self) {}
            fn description(&self) -> &str {
                "SharedCollector"
            }
        }

        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .calc()
        .build();

        pipeline.handle(&p1, &txn);
        pipeline.handle(&p2, &txn);
        pipeline.handle(&p3, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 3);

        // Check running totals stored in xdata.
        let total1 = r[0].0.xdata.as_ref().unwrap().total.as_ref().unwrap();
        assert_eq!(total1.value(), Decimal::from(50));

        let total2 = r[1].0.xdata.as_ref().unwrap().total.as_ref().unwrap();
        assert_eq!(total2.value(), Decimal::from(150));

        let total3 = r[2].0.xdata.as_ref().unwrap().total.as_ref().unwrap();
        assert_eq!(total3.value(), Decimal::from(175));

        // Check count.
        assert_eq!(r[2].0.xdata.as_ref().unwrap().count, 3);
    }

    // -- SortHandler --------------------------------------------------------

    #[test]
    fn sort_handler_sorts_by_account_name() {
        let mut tree = AccountTree::new();
        let p_c = make_posting(&mut tree, "Expenses:Zulu", 10);
        let p_a = make_posting(&mut tree, "Expenses:Alpha", 20);
        let p_b = make_posting(&mut tree, "Expenses:Mike", 30);
        let txn = make_transaction((2024, 1, 15), "Test");

        use std::cell::RefCell;
        use std::rc::Rc;

        let results: Rc<RefCell<Vec<(Posting, Transaction)>>> =
            Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);

        struct SharedCollector {
            results: Rc<RefCell<Vec<(Posting, Transaction)>>>,
        }
        impl PostHandler for SharedCollector {
            fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
                self.results.borrow_mut().push((posting.clone(), transaction.clone()));
            }
            fn flush(&mut self) {}
            fn description(&self) -> &str {
                "SharedCollector"
            }
        }

        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .sort(|p, _t| p.account_name())
        .build();

        pipeline.handle(&p_c, &txn);
        pipeline.handle(&p_a, &txn);
        pipeline.handle(&p_b, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0.account_name(), "Expenses:Alpha");
        assert_eq!(r[1].0.account_name(), "Expenses:Mike");
        assert_eq!(r[2].0.account_name(), "Expenses:Zulu");
    }

    #[test]
    fn sort_handler_reverse() {
        let mut tree = AccountTree::new();
        let p_a = make_posting(&mut tree, "A", 1);
        let p_b = make_posting(&mut tree, "B", 2);
        let p_c = make_posting(&mut tree, "C", 3);
        let txn = make_transaction((2024, 1, 1), "Test");

        use std::cell::RefCell;
        use std::rc::Rc;

        let results: Rc<RefCell<Vec<(Posting, Transaction)>>> =
            Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);

        struct SharedCollector {
            results: Rc<RefCell<Vec<(Posting, Transaction)>>>,
        }
        impl PostHandler for SharedCollector {
            fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
                self.results.borrow_mut().push((posting.clone(), transaction.clone()));
            }
            fn flush(&mut self) {}
            fn description(&self) -> &str {
                "SharedCollector"
            }
        }

        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .sort_reverse(|p, _t| p.account_name())
        .build();

        pipeline.handle(&p_a, &txn);
        pipeline.handle(&p_b, &txn);
        pipeline.handle(&p_c, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r[0].0.account_name(), "C");
        assert_eq!(r[1].0.account_name(), "B");
        assert_eq!(r[2].0.account_name(), "A");
    }

    // -- CollapseHandler ----------------------------------------------------

    #[test]
    fn collapse_handler_merges_same_account() {
        let mut tree = AccountTree::new();
        let p1 = make_posting(&mut tree, "Expenses:Food", 30);
        let p2 = make_posting(&mut tree, "Expenses:Food", 20);
        let p3 = make_posting(&mut tree, "Expenses:Rent", 500);
        let txn = make_transaction((2024, 1, 15), "Various");

        use std::cell::RefCell;
        use std::rc::Rc;

        let results: Rc<RefCell<Vec<(Posting, Transaction)>>> =
            Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);

        struct SharedCollector {
            results: Rc<RefCell<Vec<(Posting, Transaction)>>>,
        }
        impl PostHandler for SharedCollector {
            fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
                self.results.borrow_mut().push((posting.clone(), transaction.clone()));
            }
            fn flush(&mut self) {}
            fn description(&self) -> &str {
                "SharedCollector"
            }
        }

        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .collapse()
        .build();

        pipeline.handle(&p1, &txn);
        pipeline.handle(&p2, &txn);
        pipeline.handle(&p3, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2); // Two distinct accounts

        // Find the Food entry and check consolidated amount.
        let food = r.iter().find(|(p, _)| p.account_name() == "Expenses:Food").unwrap();
        assert_eq!(food.0.amount.as_ref().unwrap().value(), Decimal::from(50));

        let rent = r.iter().find(|(p, _)| p.account_name() == "Expenses:Rent").unwrap();
        assert_eq!(rent.0.amount.as_ref().unwrap().value(), Decimal::from(500));
    }

    // -- Pipeline composition -----------------------------------------------

    #[test]
    fn pipeline_filter_sort_collect() {
        let mut tree = AccountTree::new();
        let p_food = make_posting(&mut tree, "Expenses:Food", 50);
        let p_cash = make_posting(&mut tree, "Assets:Cash", -50);
        let p_rent = make_posting(&mut tree, "Expenses:Rent", 1000);
        let p_bank = make_posting(&mut tree, "Assets:Bank", -1000);
        let txn = make_transaction((2024, 1, 15), "Various");

        use std::cell::RefCell;
        use std::rc::Rc;

        let results: Rc<RefCell<Vec<(Posting, Transaction)>>> =
            Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);

        struct SharedCollector {
            results: Rc<RefCell<Vec<(Posting, Transaction)>>>,
        }
        impl PostHandler for SharedCollector {
            fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
                self.results.borrow_mut().push((posting.clone(), transaction.clone()));
            }
            fn flush(&mut self) {}
            fn description(&self) -> &str {
                "SharedCollector"
            }
        }

        // Pipeline: filter expenses only → sort by account → collect.
        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .sort(|p, _t| p.account_name())
        .filter(|p, _t| p.account_name().starts_with("Expenses"))
        .build();

        pipeline.handle(&p_food, &txn);
        pipeline.handle(&p_cash, &txn);
        pipeline.handle(&p_rent, &txn);
        pipeline.handle(&p_bank, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2);
        // Should be sorted alphabetically.
        assert_eq!(r[0].0.account_name(), "Expenses:Food");
        assert_eq!(r[1].0.account_name(), "Expenses:Rent");
    }

    #[test]
    fn pipeline_filter_calc_collect() {
        let mut tree = AccountTree::new();
        let p1 = make_posting(&mut tree, "Expenses:Food", 50);
        let p2 = make_posting(&mut tree, "Assets:Cash", -50);
        let p3 = make_posting(&mut tree, "Expenses:Rent", 100);
        let txn = make_transaction((2024, 1, 15), "Various");

        use std::cell::RefCell;
        use std::rc::Rc;

        let results: Rc<RefCell<Vec<(Posting, Transaction)>>> =
            Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);

        struct SharedCollector {
            results: Rc<RefCell<Vec<(Posting, Transaction)>>>,
        }
        impl PostHandler for SharedCollector {
            fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
                self.results.borrow_mut().push((posting.clone(), transaction.clone()));
            }
            fn flush(&mut self) {}
            fn description(&self) -> &str {
                "SharedCollector"
            }
        }

        // Pipeline: filter expenses → calc running total → collect.
        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .calc()
        .filter(|p, _t| p.account_name().starts_with("Expenses"))
        .build();

        pipeline.handle(&p1, &txn);
        pipeline.handle(&p2, &txn);
        pipeline.handle(&p3, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2); // Only expenses passed filter.

        // Running total only includes the filtered postings.
        let total1 = r[0].0.xdata.as_ref().unwrap().total.as_ref().unwrap();
        assert_eq!(total1.value(), Decimal::from(50));

        let total2 = r[1].0.xdata.as_ref().unwrap().total.as_ref().unwrap();
        assert_eq!(total2.value(), Decimal::from(150));
    }

    #[test]
    fn pipeline_collapse_sort_collect() {
        let mut tree = AccountTree::new();
        let p1 = make_posting(&mut tree, "Expenses:Food", 30);
        let p2 = make_posting(&mut tree, "Expenses:Rent", 500);
        let p3 = make_posting(&mut tree, "Expenses:Food", 20);
        let txn = make_transaction((2024, 1, 15), "Various");

        use std::cell::RefCell;
        use std::rc::Rc;

        let results: Rc<RefCell<Vec<(Posting, Transaction)>>> =
            Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);

        struct SharedCollector {
            results: Rc<RefCell<Vec<(Posting, Transaction)>>>,
        }
        impl PostHandler for SharedCollector {
            fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
                self.results.borrow_mut().push((posting.clone(), transaction.clone()));
            }
            fn flush(&mut self) {}
            fn description(&self) -> &str {
                "SharedCollector"
            }
        }

        // Pipeline: collapse by account → sort → collect.
        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .sort(|p, _t| p.account_name())
        .collapse()
        .build();

        pipeline.handle(&p1, &txn);
        pipeline.handle(&p2, &txn);
        pipeline.handle(&p3, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2);
        // Sorted by account name.
        assert_eq!(r[0].0.account_name(), "Expenses:Food");
        assert_eq!(r[0].0.amount.as_ref().unwrap().value(), Decimal::from(50));
        assert_eq!(r[1].0.account_name(), "Expenses:Rent");
        assert_eq!(r[1].0.amount.as_ref().unwrap().value(), Decimal::from(500));
    }

    // -- Description --------------------------------------------------------

    #[test]
    fn handler_descriptions() {
        assert_eq!(CollectHandler::new().description(), "CollectHandler");
        assert_eq!(CountHandler::new().description(), "CountHandler");

        let filter = FilterHandler::new(|_, _| true, Box::new(CountHandler::new()));
        assert_eq!(filter.description(), "FilterHandler");

        let calc = CalcHandler::new(Box::new(CountHandler::new()));
        assert_eq!(calc.description(), "CalcHandler");

        let sort = SortHandler::new(|_, _| String::new(), false, Box::new(CountHandler::new()));
        assert_eq!(sort.description(), "SortHandler");

        let collapse = CollapseHandler::new(Box::new(CountHandler::new()));
        assert_eq!(collapse.description(), "CollapseHandler");
    }
}
