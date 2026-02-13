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

use chrono::NaiveDate;
use ledger_math::amount::Amount;
use ledger_math::datetime::Period;

use crate::expr::{ExprContext, Expression, Value};
use crate::posting::Posting;
use crate::transaction::Transaction;

/// Predicate function type for filtering postings.
type PostingPredicate = dyn Fn(&Posting, &Transaction) -> bool;

/// Key extraction function type for sorting postings.
type PostingSortKey = dyn Fn(&Posting, &Transaction) -> String;

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
    predicate: Box<PostingPredicate>,
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
    sort_key: Box<PostingSortKey>,
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
// SubtotalHandler — groups postings by account and produces subtotals
// ---------------------------------------------------------------------------

/// Handler that groups postings by account name and produces one
/// synthetic posting per account with the subtotaled amount on flush.
pub struct SubtotalHandler {
    subtotals: HashMap<String, Amount>,
    /// Keep a representative (posting, transaction) per account for
    /// reconstructing the forwarded posting.
    representatives: HashMap<String, (Posting, Transaction)>,
    next: Box<dyn PostHandler>,
}

impl SubtotalHandler {
    pub fn new(next: Box<dyn PostHandler>) -> Self {
        Self {
            subtotals: HashMap::new(),
            representatives: HashMap::new(),
            next,
        }
    }
}

impl PostHandler for SubtotalHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        let account_name = posting.account_name();

        if let Some(total) = self.subtotals.get_mut(&account_name) {
            if let Some(ref amount) = posting.amount {
                let _ = total.add_amount(amount);
            }
        } else {
            let initial = posting.amount.clone().unwrap_or_else(Amount::null);
            self.subtotals.insert(account_name.clone(), initial);
            self.representatives
                .insert(account_name, (posting.clone(), transaction.clone()));
        }
    }

    fn flush(&mut self) {
        for (name, total) in self.subtotals.drain() {
            if let Some((mut posting, transaction)) = self.representatives.remove(&name) {
                posting.amount = Some(total);
                self.next.handle(&posting, &transaction);
            }
        }
        self.next.flush();
    }

    fn description(&self) -> &str {
        "SubtotalHandler"
    }
}

// ---------------------------------------------------------------------------
// IntervalHandler — groups postings into time intervals
// ---------------------------------------------------------------------------

/// Handler that groups postings into time intervals (daily, weekly,
/// monthly, etc.) and subtotals within each interval. On flush the
/// final period's subtotals are emitted.
pub struct IntervalHandler {
    period: Period,
    current_period_start: Option<NaiveDate>,
    current_subtotals: HashMap<String, Amount>,
    current_representatives: HashMap<String, (Posting, Transaction)>,
    next: Box<dyn PostHandler>,
}

impl IntervalHandler {
    pub fn new(period: Period, next: Box<dyn PostHandler>) -> Self {
        Self {
            period,
            current_period_start: None,
            current_subtotals: HashMap::new(),
            current_representatives: HashMap::new(),
            next,
        }
    }

    /// Determine the period start date for a given date by finding the
    /// beginning of the period that contains the date.
    fn period_start_for(&self, date: NaiveDate) -> NaiveDate {
        use chrono::Datelike;
        match &self.period {
            Period::Daily(_) => date,
            Period::Weekly(_) => {
                // Start of ISO week (Monday)
                let weekday = date.weekday().num_days_from_monday();
                date - chrono::Duration::days(weekday as i64)
            }
            Period::Biweekly => {
                let weekday = date.weekday().num_days_from_monday();
                date - chrono::Duration::days(weekday as i64)
            }
            Period::Monthly(_) | Period::Bimonthly => {
                NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap_or(date)
            }
            Period::Quarterly(_) => {
                let quarter_month = ((date.month() - 1) / 3) * 3 + 1;
                NaiveDate::from_ymd_opt(date.year(), quarter_month, 1).unwrap_or(date)
            }
            Period::Yearly(_) => {
                NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap_or(date)
            }
        }
    }

    /// Flush the current period's subtotals to the next handler.
    fn flush_current_period(&mut self) {
        for (name, total) in self.current_subtotals.drain() {
            if let Some((mut posting, transaction)) = self.current_representatives.remove(&name) {
                posting.amount = Some(total);
                self.next.handle(&posting, &transaction);
            }
        }
    }

    /// Accumulate a posting into the current period.
    fn accumulate(&mut self, posting: &Posting, transaction: &Transaction) {
        let account_name = posting.account_name();

        if let Some(total) = self.current_subtotals.get_mut(&account_name) {
            if let Some(ref amount) = posting.amount {
                let _ = total.add_amount(amount);
            }
        } else {
            let initial = posting.amount.clone().unwrap_or_else(Amount::null);
            self.current_subtotals.insert(account_name.clone(), initial);
            self.current_representatives
                .insert(account_name, (posting.clone(), transaction.clone()));
        }
    }
}

impl PostHandler for IntervalHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        let date = transaction.date;
        let period_start = self.period_start_for(date);

        match self.current_period_start {
            Some(current_start) if current_start != period_start => {
                // New period — flush the old one first.
                self.flush_current_period();
                self.current_period_start = Some(period_start);
                self.accumulate(posting, transaction);
            }
            None => {
                self.current_period_start = Some(period_start);
                self.accumulate(posting, transaction);
            }
            Some(_) => {
                // Same period — just accumulate.
                self.accumulate(posting, transaction);
            }
        }
    }

    fn flush(&mut self) {
        self.flush_current_period();
        self.next.flush();
    }

    fn description(&self) -> &str {
        "IntervalHandler"
    }
}

// ---------------------------------------------------------------------------
// DisplayFilterHandler — expression-based display predicate
// ---------------------------------------------------------------------------

/// Handler that evaluates an [`Expression`] predicate against each
/// posting and only forwards those for which the expression is truthy.
///
/// Unlike [`FilterHandler`] (which uses closures), this handler uses
/// the expression engine so predicates can reference computed values
/// like running totals.
pub struct DisplayFilterHandler {
    predicate: Expression,
    next: Box<dyn PostHandler>,
}

impl DisplayFilterHandler {
    pub fn new(predicate: Expression, next: Box<dyn PostHandler>) -> Self {
        Self { predicate, next }
    }
}

impl PostHandler for DisplayFilterHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        // Build a minimal context exposing the posting's amount and
        // running total so the expression can reference them.
        let mut ctx = ExprContext::new();

        if let Some(ref amount) = posting.amount {
            ctx.set_variable("amount".to_string(), Value::Amount(amount.clone()));
        } else {
            ctx.set_variable("amount".to_string(), Value::Null);
        }

        if let Some(ref xdata) = posting.xdata {
            if let Some(ref total) = xdata.total {
                ctx.set_variable("total".to_string(), Value::Amount(total.clone()));
            }
        }

        ctx.set_variable(
            "account".to_string(),
            Value::String(posting.account_name()),
        );
        ctx.set_variable(
            "payee".to_string(),
            Value::String(transaction.payee.clone()),
        );

        match self.predicate.evaluate(&ctx) {
            Ok(value) if value.is_truthy() => {
                self.next.handle(posting, transaction);
            }
            _ => {}
        }
    }

    fn flush(&mut self) {
        self.next.flush();
    }

    fn description(&self) -> &str {
        "DisplayFilterHandler"
    }
}

// ---------------------------------------------------------------------------
// RelatedHandler — forwards related postings from the same transaction
// ---------------------------------------------------------------------------

/// For each posting received, forwards the *other* postings in the
/// same transaction (the "related" postings). If `also_matching` is
/// `true`, the original posting is forwarded too.
pub struct RelatedHandler {
    also_matching: bool,
    next: Box<dyn PostHandler>,
}

impl RelatedHandler {
    pub fn new(also_matching: bool, next: Box<dyn PostHandler>) -> Self {
        Self { also_matching, next }
    }
}

impl PostHandler for RelatedHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        if self.also_matching {
            self.next.handle(posting, transaction);
        }

        // Forward all other postings from the same transaction.
        let posting_account = posting.account_name();
        let posting_seq = posting.sequence;

        for related in &transaction.postings {
            // Skip the original posting (match by sequence and account).
            if related.sequence == posting_seq && related.account_name() == posting_account {
                continue;
            }
            self.next.handle(related, transaction);
        }
    }

    fn flush(&mut self) {
        self.next.flush();
    }

    fn description(&self) -> &str {
        "RelatedHandler"
    }
}

// ---------------------------------------------------------------------------
// TruncateHandler — limits output to first N or last N postings
// ---------------------------------------------------------------------------

/// Handler that limits output to the first N (`head_count`) or last N
/// (`tail_count`) postings. If both are set, head takes precedence.
pub struct TruncateHandler {
    head_count: Option<usize>,
    tail_count: Option<usize>,
    buffer: Vec<(Posting, Transaction)>,
    count: usize,
    next: Box<dyn PostHandler>,
}

impl TruncateHandler {
    pub fn head(n: usize, next: Box<dyn PostHandler>) -> Self {
        Self {
            head_count: Some(n),
            tail_count: None,
            buffer: Vec::new(),
            count: 0,
            next,
        }
    }

    pub fn tail(n: usize, next: Box<dyn PostHandler>) -> Self {
        Self {
            head_count: None,
            tail_count: Some(n),
            buffer: Vec::new(),
            count: 0,
            next,
        }
    }
}

impl PostHandler for TruncateHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        if let Some(head) = self.head_count {
            // Head mode: forward immediately until we've sent enough.
            if self.count < head {
                self.next.handle(posting, transaction);
                self.count += 1;
            }
        } else {
            // Tail mode: buffer everything; we'll pick the last N on flush.
            self.buffer.push((posting.clone(), transaction.clone()));
        }
    }

    fn flush(&mut self) {
        if let Some(tail) = self.tail_count {
            let skip = self.buffer.len().saturating_sub(tail);
            for (posting, transaction) in self.buffer.drain(..).skip(skip) {
                self.next.handle(&posting, &transaction);
            }
        }
        self.next.flush();
    }

    fn description(&self) -> &str {
        "TruncateHandler"
    }
}

// ---------------------------------------------------------------------------
// InvertHandler — negates all posting amounts
// ---------------------------------------------------------------------------

/// Handler that negates every posting's amount before forwarding it
/// to the next handler.
pub struct InvertHandler {
    next: Box<dyn PostHandler>,
}

impl InvertHandler {
    pub fn new(next: Box<dyn PostHandler>) -> Self {
        Self { next }
    }
}

impl PostHandler for InvertHandler {
    fn handle(&mut self, posting: &Posting, transaction: &Transaction) {
        let mut inverted = posting.clone();
        if let Some(ref amount) = inverted.amount {
            inverted.amount = Some(amount.negated());
        }
        self.next.handle(&inverted, transaction);
    }

    fn flush(&mut self) {
        self.next.flush();
    }

    fn description(&self) -> &str {
        "InvertHandler"
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

    /// Wrap the current pipeline with a [`SubtotalHandler`].
    pub fn subtotal(self) -> Self {
        Self { current: Box::new(SubtotalHandler::new(self.current)) }
    }

    /// Wrap the current pipeline with an [`IntervalHandler`].
    pub fn interval(self, period: Period) -> Self {
        Self { current: Box::new(IntervalHandler::new(period, self.current)) }
    }

    /// Wrap the current pipeline with a [`DisplayFilterHandler`].
    pub fn display_filter(self, expr: Expression) -> Self {
        Self { current: Box::new(DisplayFilterHandler::new(expr, self.current)) }
    }

    /// Wrap the current pipeline with a [`RelatedHandler`].
    pub fn related(self, also_matching: bool) -> Self {
        Self { current: Box::new(RelatedHandler::new(also_matching, self.current)) }
    }

    /// Wrap the current pipeline with a [`TruncateHandler`] that keeps the first N.
    pub fn truncate_head(self, n: usize) -> Self {
        Self { current: Box::new(TruncateHandler::head(n, self.current)) }
    }

    /// Wrap the current pipeline with a [`TruncateHandler`] that keeps the last N.
    pub fn truncate_tail(self, n: usize) -> Self {
        Self { current: Box::new(TruncateHandler::tail(n, self.current)) }
    }

    /// Wrap the current pipeline with an [`InvertHandler`].
    pub fn invert(self) -> Self {
        Self { current: Box::new(InvertHandler::new(self.current)) }
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

    // -- SubtotalHandler ----------------------------------------------------

    #[test]
    fn subtotal_handler_groups_by_account() {
        let mut tree = AccountTree::new();
        let p1 = make_posting(&mut tree, "Expenses:Food", 30);
        let p2 = make_posting(&mut tree, "Assets:Cash", -30);
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

        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .subtotal()
        .build();

        pipeline.handle(&p1, &txn);
        pipeline.handle(&p2, &txn);
        pipeline.handle(&p3, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2);

        let food = r.iter().find(|(p, _)| p.account_name() == "Expenses:Food").unwrap();
        assert_eq!(food.0.amount.as_ref().unwrap().value(), Decimal::from(50));

        let cash = r.iter().find(|(p, _)| p.account_name() == "Assets:Cash").unwrap();
        assert_eq!(cash.0.amount.as_ref().unwrap().value(), Decimal::from(-30));
    }

    // -- IntervalHandler ----------------------------------------------------

    #[test]
    fn interval_handler_groups_by_month() {
        let mut tree = AccountTree::new();
        let p_jan = make_posting(&mut tree, "Expenses:Food", 100);
        let p_jan2 = make_posting(&mut tree, "Expenses:Food", 50);
        let p_feb = make_posting(&mut tree, "Expenses:Food", 200);
        let p_mar = make_posting(&mut tree, "Expenses:Food", 300);
        let txn_jan = make_transaction((2024, 1, 10), "Jan expense");
        let txn_jan2 = make_transaction((2024, 1, 20), "Jan expense 2");
        let txn_feb = make_transaction((2024, 2, 15), "Feb expense");
        let txn_mar = make_transaction((2024, 3, 5), "Mar expense");

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
        .interval(Period::Monthly(1))
        .build();

        pipeline.handle(&p_jan, &txn_jan);
        pipeline.handle(&p_jan2, &txn_jan2);
        pipeline.handle(&p_feb, &txn_feb);
        pipeline.handle(&p_mar, &txn_mar);
        pipeline.flush();

        let r = results.borrow();
        // 3 groups: Jan (100+50=150), Feb (200), Mar (300)
        assert_eq!(r.len(), 3);

        // Results come out in period order: Jan first, then Feb, then Mar.
        assert_eq!(r[0].0.amount.as_ref().unwrap().value(), Decimal::from(150));
        assert_eq!(r[1].0.amount.as_ref().unwrap().value(), Decimal::from(200));
        assert_eq!(r[2].0.amount.as_ref().unwrap().value(), Decimal::from(300));
    }

    // -- DisplayFilterHandler -----------------------------------------------

    #[test]
    fn display_filter_handler_filters_by_expression() {
        let mut tree = AccountTree::new();
        let p_big = make_posting(&mut tree, "Expenses:Food", 100);
        let p_small = make_posting(&mut tree, "Expenses:Snack", 5);
        let p_medium = make_posting(&mut tree, "Expenses:Lunch", 25);
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

        // Predicate: amount > 10
        let expr = Expression::parse("amount > 10").unwrap();

        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .display_filter(expr)
        .build();

        pipeline.handle(&p_big, &txn);
        pipeline.handle(&p_small, &txn);
        pipeline.handle(&p_medium, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0.account_name(), "Expenses:Food");
        assert_eq!(r[1].0.account_name(), "Expenses:Lunch");
    }

    // -- RelatedHandler -----------------------------------------------------

    #[test]
    fn related_handler_forwards_related_postings() {
        let mut tree = AccountTree::new();
        let p_food = make_posting(&mut tree, "Expenses:Food", 50);
        let p_cash = make_posting(&mut tree, "Assets:Cash", -50);

        // Build a transaction that contains both postings.
        let mut txn = make_transaction((2024, 1, 15), "Grocery");
        txn.postings.push(p_food.clone());
        txn.postings.push(p_cash.clone());

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

        // Without also_matching: should forward only the *other* postings.
        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .related(false)
        .build();

        pipeline.handle(&p_food, &txn);
        pipeline.flush();

        let r = results.borrow();
        // p_food is not forwarded (also_matching=false), only p_cash.
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0.account_name(), "Assets:Cash");
    }

    #[test]
    fn related_handler_also_matching() {
        let mut tree = AccountTree::new();
        let mut p_food = make_posting(&mut tree, "Expenses:Food", 50);
        p_food.sequence = 1;
        let mut p_cash = make_posting(&mut tree, "Assets:Cash", -50);
        p_cash.sequence = 2;

        let mut txn = make_transaction((2024, 1, 15), "Grocery");
        txn.postings.push(p_food.clone());
        txn.postings.push(p_cash.clone());

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
        .related(true)
        .build();

        pipeline.handle(&p_food, &txn);
        pipeline.flush();

        let r = results.borrow();
        // also_matching=true: both the original and the related posting.
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0.account_name(), "Expenses:Food");
        assert_eq!(r[1].0.account_name(), "Assets:Cash");
    }

    // -- TruncateHandler ----------------------------------------------------

    #[test]
    fn truncate_handler_head() {
        let mut tree = AccountTree::new();
        let postings: Vec<Posting> = (1..=5)
            .map(|i| make_posting(&mut tree, &format!("Account:{}", i), i * 10))
            .collect();
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
        .truncate_head(2)
        .build();

        for p in &postings {
            pipeline.handle(p, &txn);
        }
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0.amount.as_ref().unwrap().value(), Decimal::from(10));
        assert_eq!(r[1].0.amount.as_ref().unwrap().value(), Decimal::from(20));
    }

    #[test]
    fn truncate_handler_tail() {
        let mut tree = AccountTree::new();
        let postings: Vec<Posting> = (1..=5)
            .map(|i| make_posting(&mut tree, &format!("Account:{}", i), i * 10))
            .collect();
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
        .truncate_tail(2)
        .build();

        for p in &postings {
            pipeline.handle(p, &txn);
        }
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0.amount.as_ref().unwrap().value(), Decimal::from(40));
        assert_eq!(r[1].0.amount.as_ref().unwrap().value(), Decimal::from(50));
    }

    // -- InvertHandler ------------------------------------------------------

    #[test]
    fn invert_handler_negates_amounts() {
        let mut tree = AccountTree::new();
        let p1 = make_posting(&mut tree, "Expenses:Food", 50);
        let p2 = make_posting(&mut tree, "Assets:Cash", -30);
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
        .invert()
        .build();

        pipeline.handle(&p1, &txn);
        pipeline.handle(&p2, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0.amount.as_ref().unwrap().value(), Decimal::from(-50));
        assert_eq!(r[1].0.amount.as_ref().unwrap().value(), Decimal::from(30));
    }

    // -- Pipeline composition with new handlers -----------------------------

    #[test]
    fn pipeline_filter_subtotal_sort_collect() {
        let mut tree = AccountTree::new();
        let p1 = make_posting(&mut tree, "Expenses:Rent", 500);
        let p2 = make_posting(&mut tree, "Expenses:Food", 30);
        let p3 = make_posting(&mut tree, "Expenses:Food", 20);
        let p4 = make_posting(&mut tree, "Assets:Cash", -550);
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

        // Pipeline: filter → subtotal → sort → collect.
        let mut pipeline = PipelineBuilder::new(Box::new(SharedCollector {
            results: results_clone,
        }))
        .sort(|p, _t| p.account_name())
        .subtotal()
        .filter(|p, _t| p.account_name().starts_with("Expenses"))
        .build();

        pipeline.handle(&p1, &txn);
        pipeline.handle(&p2, &txn);
        pipeline.handle(&p3, &txn);
        pipeline.handle(&p4, &txn);
        pipeline.flush();

        let r = results.borrow();
        assert_eq!(r.len(), 2); // Food(50) and Rent(500), sorted
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

        let subtotal = SubtotalHandler::new(Box::new(CountHandler::new()));
        assert_eq!(subtotal.description(), "SubtotalHandler");

        let interval = IntervalHandler::new(Period::Monthly(1), Box::new(CountHandler::new()));
        assert_eq!(interval.description(), "IntervalHandler");

        let expr = Expression::parse("amount > 0").unwrap();
        let display = DisplayFilterHandler::new(expr, Box::new(CountHandler::new()));
        assert_eq!(display.description(), "DisplayFilterHandler");

        let related = RelatedHandler::new(false, Box::new(CountHandler::new()));
        assert_eq!(related.description(), "RelatedHandler");

        let trunc = TruncateHandler::head(5, Box::new(CountHandler::new()));
        assert_eq!(trunc.description(), "TruncateHandler");

        let invert = InvertHandler::new(Box::new(CountHandler::new()));
        assert_eq!(invert.description(), "InvertHandler");
    }
}
