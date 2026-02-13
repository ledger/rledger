//! Period expression tokenizer and parser.
//!
//! Handles expressions like "monthly", "from 2024/01 to 2024/12",
//! "last 3 months", "every 2 weeks since 2024/01".

use chrono::{Datelike, NaiveDate};

use crate::datetime::{
    add_months_to_date, add_years_to_date, current_date, parse_date, DateInterval, Period,
    PeriodParseError,
};

// ── Token types ──────────────────────────────────────────────────────

/// Tokens produced by the period-expression lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum PeriodToken {
    // Range / relative keywords
    Since,
    Until,
    From,
    To,
    In,
    This,
    Next,
    Last,
    Every,
    Ago,
    Hence,

    // Singular time-unit keywords
    Day,
    Week,
    Month,
    Quarter,
    Year,

    // Plural time-unit keywords
    Days,
    Weeks,
    Months,
    Quarters,
    Years,

    // Period shorthand keywords
    Daily,
    Weekly,
    Biweekly,
    Monthly,
    Bimonthly,
    Quarterly,
    Yearly,

    // Day-of-week names
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,

    // Month names
    January,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,

    // Special date words
    Today,
    Yesterday,
    Tomorrow,

    // Structural
    Dash,
    Number(i32),
    DateLiteral(NaiveDate),
    Unknown(String),
}

impl PeriodToken {
    /// Return the month number (1-12) if this token is a month name.
    pub fn month_number(&self) -> Option<u32> {
        match self {
            PeriodToken::January => Some(1),
            PeriodToken::February => Some(2),
            PeriodToken::March => Some(3),
            PeriodToken::April => Some(4),
            PeriodToken::May => Some(5),
            PeriodToken::June => Some(6),
            PeriodToken::July => Some(7),
            PeriodToken::August => Some(8),
            PeriodToken::September => Some(9),
            PeriodToken::October => Some(10),
            PeriodToken::November => Some(11),
            PeriodToken::December => Some(12),
            _ => None,
        }
    }

    fn is_time_unit(&self) -> bool {
        matches!(
            self,
            PeriodToken::Day
                | PeriodToken::Days
                | PeriodToken::Week
                | PeriodToken::Weeks
                | PeriodToken::Month
                | PeriodToken::Months
                | PeriodToken::Quarter
                | PeriodToken::Quarters
                | PeriodToken::Year
                | PeriodToken::Years
        )
    }

    fn is_month_name(&self) -> bool {
        self.month_number().is_some()
    }
}

// ── Tokenizer ────────────────────────────────────────────────────────

/// Tokenize a period expression string into a sequence of [`PeriodToken`]s.
pub fn tokenize_period(input: &str) -> Vec<PeriodToken> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }

        if ch == '-' {
            // Peek ahead: if next non-whitespace is a digit this could be part
            // of a negative number, but period expressions don't use negative
            // numbers directly – treat as Dash.
            chars.next();
            tokens.push(PeriodToken::Dash);
            continue;
        }

        if ch.is_ascii_digit() {
            // Collect contiguous digits-and-separators to detect date literals.
            let word = collect_date_or_number(&mut chars);
            if let Some(date) = try_parse_date_literal(&word) {
                tokens.push(PeriodToken::DateLiteral(date));
            } else if let Ok(n) = word.parse::<i32>() {
                tokens.push(PeriodToken::Number(n));
            } else {
                tokens.push(PeriodToken::Unknown(word));
            }
            continue;
        }

        if ch.is_alphabetic() {
            let word = collect_alpha(&mut chars);
            tokens.push(keyword_token(&word));
            continue;
        }

        // Skip unrecognized characters
        chars.next();
    }

    tokens
}

/// Collect a word that may be a date literal (digits + `/` or `-` or `.`).
fn collect_date_or_number(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut buf = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() || ch == '/' || ch == '.' {
            buf.push(ch);
            chars.next();
        } else if ch == '-' {
            // Only include '-' if it looks like a date separator (digit before
            // and digit after).
            if buf.chars().last().is_some_and(|c| c.is_ascii_digit()) {
                // Peek one further
                let mut clone = chars.clone();
                clone.next(); // skip '-'
                if clone.peek().is_some_and(|c| c.is_ascii_digit()) {
                    buf.push(ch);
                    chars.next();
                } else {
                    break;
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }
    buf
}

/// Collect a run of alphabetic characters.
fn collect_alpha(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut buf = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_alphabetic() {
            buf.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    buf
}

/// Try to parse a string as a date literal (YYYY/MM/DD, YYYY-MM-DD, YYYY/MM, etc.).
///
/// Only attempts parsing when the string contains a date separator
/// (`/`, `-`, `.`), e.g. "2024/01/01", "2024/01", "2024-01-01".
///
/// Pure digit strings (even "2024" or "20240101") are returned as
/// `Number` tokens so that the parser can decide context-sensitively
/// whether they represent years.
fn try_parse_date_literal(s: &str) -> Option<NaiveDate> {
    let has_separator = s.contains('/') || s.contains('-') || s.contains('.');
    if has_separator {
        parse_date(s).ok().map(|d| d.naive_date())
    } else {
        None
    }
}

/// Map a lowercase word to the corresponding [`PeriodToken`].
fn keyword_token(word: &str) -> PeriodToken {
    match word.to_ascii_lowercase().as_str() {
        "since" | "from" => PeriodToken::Since,
        "until" | "to" => PeriodToken::Until,
        "in" => PeriodToken::In,
        "this" => PeriodToken::This,
        "next" => PeriodToken::Next,
        "last" => PeriodToken::Last,
        "every" => PeriodToken::Every,
        "ago" => PeriodToken::Ago,
        "hence" => PeriodToken::Hence,

        "day" => PeriodToken::Day,
        "week" => PeriodToken::Week,
        "month" => PeriodToken::Month,
        "quarter" => PeriodToken::Quarter,
        "year" => PeriodToken::Year,

        "days" => PeriodToken::Days,
        "weeks" => PeriodToken::Weeks,
        "months" => PeriodToken::Months,
        "quarters" => PeriodToken::Quarters,
        "years" => PeriodToken::Years,

        "daily" => PeriodToken::Daily,
        "weekly" => PeriodToken::Weekly,
        "biweekly" => PeriodToken::Biweekly,
        "monthly" => PeriodToken::Monthly,
        "bimonthly" => PeriodToken::Bimonthly,
        "quarterly" => PeriodToken::Quarterly,
        "yearly" => PeriodToken::Yearly,

        "monday" | "mon" => PeriodToken::Monday,
        "tuesday" | "tue" => PeriodToken::Tuesday,
        "wednesday" | "wed" => PeriodToken::Wednesday,
        "thursday" | "thu" => PeriodToken::Thursday,
        "friday" | "fri" => PeriodToken::Friday,
        "saturday" | "sat" => PeriodToken::Saturday,
        "sunday" | "sun" => PeriodToken::Sunday,

        "january" | "jan" => PeriodToken::January,
        "february" | "feb" => PeriodToken::February,
        "march" | "mar" => PeriodToken::March,
        "april" | "apr" => PeriodToken::April,
        "may" => PeriodToken::May,
        "june" | "jun" => PeriodToken::June,
        "july" | "jul" => PeriodToken::July,
        "august" | "aug" => PeriodToken::August,
        "september" | "sep" => PeriodToken::September,
        "october" | "oct" => PeriodToken::October,
        "november" | "nov" => PeriodToken::November,
        "december" | "dec" => PeriodToken::December,

        "today" => PeriodToken::Today,
        "yesterday" => PeriodToken::Yesterday,
        "tomorrow" => PeriodToken::Tomorrow,

        other => PeriodToken::Unknown(other.to_string()),
    }
}

// ── Parsed result ────────────────────────────────────────────────────

/// The fully-parsed result of a period expression.
#[derive(Debug, Clone, PartialEq)]
pub struct PeriodExpression {
    /// The recurring period/interval (daily, weekly, every 2 months, etc.)
    pub period: Option<Period>,
    /// Start-date constraint (inclusive).
    pub since: Option<NaiveDate>,
    /// End-date constraint (exclusive unless from dash-range).
    pub until: Option<NaiveDate>,
    /// Whether the end date is inclusive (dash-range semantics).
    pub end_inclusive: bool,
}

// ── Parser ───────────────────────────────────────────────────────────

/// Parse a token stream into a [`PeriodExpression`].
pub fn parse_period_expression(
    tokens: &[PeriodToken],
) -> Result<PeriodExpression, PeriodParseError> {
    let mut parser = Parser::new(tokens);
    parser.parse()
}

struct Parser<'a> {
    tokens: &'a [PeriodToken],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(tokens: &'a [PeriodToken]) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&PeriodToken> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&PeriodToken> {
        let tok = self.tokens.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn parse(&mut self) -> Result<PeriodExpression, PeriodParseError> {
        let mut result = PeriodExpression {
            period: None,
            since: None,
            until: None,
            end_inclusive: false,
        };

        while !self.at_end() {
            let tok = self.peek().unwrap().clone();
            match tok {
                // ── Period shorthand keywords ────────────────────────
                PeriodToken::Daily => {
                    self.advance();
                    result.period = Some(Period::Daily(1));
                }
                PeriodToken::Weekly => {
                    self.advance();
                    result.period = Some(Period::Weekly(1));
                }
                PeriodToken::Biweekly => {
                    self.advance();
                    result.period = Some(Period::Biweekly);
                }
                PeriodToken::Monthly => {
                    self.advance();
                    result.period = Some(Period::Monthly(1));
                }
                PeriodToken::Bimonthly => {
                    self.advance();
                    result.period = Some(Period::Bimonthly);
                }
                PeriodToken::Quarterly => {
                    self.advance();
                    result.period = Some(Period::Quarterly(1));
                }
                PeriodToken::Yearly => {
                    self.advance();
                    result.period = Some(Period::Yearly(1));
                }

                // ── "every" N unit ───────────────────────────────────
                PeriodToken::Every => {
                    self.advance();
                    result.period = Some(self.parse_every()?);
                }

                // ── "since" / "from" ─────────────────────────────────
                PeriodToken::Since | PeriodToken::From => {
                    self.advance();
                    result.since = Some(self.parse_date_expr()?);
                }

                // ── "until" / "to" ───────────────────────────────────
                PeriodToken::Until | PeriodToken::To => {
                    self.advance();
                    result.until = Some(self.parse_date_expr()?);
                }

                // ── "in" YEAR or "in" MONTH ───────────────────────────
                PeriodToken::In => {
                    self.advance();
                    // Peek to decide: month name → month range, number → year
                    if let Some(tok) = self.peek() {
                        if tok.is_month_name() {
                            let date = self.parse_date_expr()?;
                            result.since = Some(date);
                            result.until = Some(add_months_to_date(date, 1));
                        } else {
                            let date = self.parse_date_expr()?;
                            result.since = Some(date);
                            result.until = Some(add_years_to_date(date, 1));
                        }
                    } else {
                        return Err(PeriodParseError::InvalidFormat(
                            "expected year or month after 'in'".into(),
                        ));
                    }
                }

                // ── "this" / "next" / "last" ─────────────────────────
                PeriodToken::This | PeriodToken::Next | PeriodToken::Last => {
                    self.parse_relative_range(&mut result)?;
                }

                // ── Date literal or Number (start of dash-range or since) ──
                PeriodToken::DateLiteral(_) | PeriodToken::Number(_) => {
                    let date = self.parse_date_expr()?;
                    if self.peek() == Some(&PeriodToken::Dash) {
                        self.advance(); // consume '-'
                        let end = self.parse_date_expr()?;
                        result.since = Some(date);
                        result.until = Some(end);
                        result.end_inclusive = true;
                    } else {
                        // Bare date acts as "since"
                        result.since = Some(date);
                    }
                }

                // ── Month name (acts as date expr) ───────────────────
                ref t if t.is_month_name() => {
                    let date = self.parse_date_expr()?;
                    if self.peek() == Some(&PeriodToken::Dash) {
                        self.advance();
                        let end = self.parse_date_expr()?;
                        result.since = Some(date);
                        result.until = Some(end);
                        result.end_inclusive = true;
                    } else {
                        result.since = Some(date);
                        // Month-name alone → since=1st of month, until=1st of next month
                        result.until =
                            Some(add_months_to_date(date, 1));
                    }
                }

                // ── Today / Yesterday / Tomorrow as date expr ────────
                PeriodToken::Today | PeriodToken::Yesterday | PeriodToken::Tomorrow => {
                    let date = self.parse_date_expr()?;
                    result.since = Some(date);
                }

                PeriodToken::Unknown(ref s) => {
                    return Err(PeriodParseError::UnknownKeyword(s.clone()));
                }
                _ => {
                    self.advance(); // skip unexpected
                }
            }
        }

        Ok(result)
    }

    /// Parse "every [N] <unit>"
    fn parse_every(&mut self) -> Result<Period, PeriodParseError> {
        match self.peek() {
            Some(PeriodToken::Number(n)) => {
                let n = *n as u32;
                self.advance();
                self.parse_time_unit_as_period(n)
            }
            Some(tok) if tok.is_time_unit() => self.parse_time_unit_as_period(1),
            _ => Err(PeriodParseError::InvalidFormat(
                "expected number or time unit after 'every'".into(),
            )),
        }
    }

    /// Consume a time-unit token and return the corresponding Period with
    /// the given multiplier.
    fn parse_time_unit_as_period(&mut self, n: u32) -> Result<Period, PeriodParseError> {
        match self.advance() {
            Some(PeriodToken::Day | PeriodToken::Days) => Ok(Period::Daily(n)),
            Some(PeriodToken::Week | PeriodToken::Weeks) => Ok(Period::Weekly(n)),
            Some(PeriodToken::Month | PeriodToken::Months) => Ok(Period::Monthly(n)),
            Some(PeriodToken::Quarter | PeriodToken::Quarters) => Ok(Period::Quarterly(n)),
            Some(PeriodToken::Year | PeriodToken::Years) => Ok(Period::Yearly(n)),
            _ => Err(PeriodParseError::InvalidFormat(
                "expected time unit (day/week/month/quarter/year)".into(),
            )),
        }
    }

    /// Parse a date expression and resolve it to a concrete NaiveDate.
    fn parse_date_expr(&mut self) -> Result<NaiveDate, PeriodParseError> {
        let today = current_date();
        match self.advance().cloned() {
            Some(PeriodToken::DateLiteral(d)) => Ok(d),

            Some(PeriodToken::Today) => Ok(today),
            Some(PeriodToken::Yesterday) => Ok(today - chrono::Duration::days(1)),
            Some(PeriodToken::Tomorrow) => Ok(today + chrono::Duration::days(1)),

            Some(PeriodToken::This) => self.resolve_this(today),
            Some(PeriodToken::Next) => self.resolve_next(today),
            Some(PeriodToken::Last) => self.resolve_last(today),

            Some(PeriodToken::Number(n)) => {
                // Could be:  N <unit> ago/hence  |  bare year
                if let Some(tok) = self.peek() {
                    if tok.is_time_unit() {
                        return self.resolve_relative_offset(n, today);
                    }
                }
                // Treat as year
                if n > 31 {
                    Ok(NaiveDate::from_ymd_opt(n, 1, 1).ok_or_else(|| {
                        PeriodParseError::InvalidFormat(format!("invalid year {}", n))
                    })?)
                } else {
                    Err(PeriodParseError::InvalidFormat(format!(
                        "ambiguous number {} in date expression",
                        n
                    )))
                }
            }

            Some(ref tok) if tok.is_month_name() => {
                let m = tok.month_number().unwrap();
                // Check if followed by a year number
                if let Some(PeriodToken::Number(y)) = self.peek() {
                    let y = *y;
                    if y > 31 {
                        self.advance();
                        return NaiveDate::from_ymd_opt(y, m, 1).ok_or_else(|| {
                            PeriodParseError::InvalidFormat(format!(
                                "invalid date: {} {}",
                                tok.month_number().unwrap(),
                                y
                            ))
                        });
                    }
                }
                Ok(NaiveDate::from_ymd_opt(today.year(), m, 1).ok_or_else(|| {
                    PeriodParseError::InvalidFormat("invalid month".into())
                })?)
            }

            Some(other) => Err(PeriodParseError::InvalidFormat(format!(
                "unexpected token in date expression: {:?}",
                other
            ))),
            None => Err(PeriodParseError::InvalidFormat(
                "unexpected end of expression".into(),
            )),
        }
    }

    /// "this" <unit> → start of current period
    fn resolve_this(&mut self, today: NaiveDate) -> Result<NaiveDate, PeriodParseError> {
        match self.advance() {
            Some(PeriodToken::Day) => Ok(today),
            Some(PeriodToken::Week) => {
                let wd = today.weekday().num_days_from_monday();
                Ok(today - chrono::Duration::days(wd as i64))
            }
            Some(PeriodToken::Month) => Ok(NaiveDate::from_ymd_opt(today.year(), today.month(), 1)
                .ok_or_else(|| PeriodParseError::InvalidFormat("invalid month".into()))?),
            Some(PeriodToken::Quarter) => {
                let qm = quarter_start_month(today.month());
                Ok(NaiveDate::from_ymd_opt(today.year(), qm, 1)
                    .ok_or_else(|| PeriodParseError::InvalidFormat("invalid quarter".into()))?)
            }
            Some(PeriodToken::Year) => Ok(NaiveDate::from_ymd_opt(today.year(), 1, 1)
                .ok_or_else(|| PeriodParseError::InvalidFormat("invalid year".into()))?),
            _ => Err(PeriodParseError::InvalidFormat(
                "expected time unit after 'this'".into(),
            )),
        }
    }

    /// "next" <unit> → start of next period
    fn resolve_next(&mut self, today: NaiveDate) -> Result<NaiveDate, PeriodParseError> {
        match self.peek().cloned() {
            // "next N <unit>" → range from today to today + N units
            Some(PeriodToken::Number(_)) => self.resolve_next_n(today),
            _ => match self.advance() {
                Some(PeriodToken::Day) => Ok(today + chrono::Duration::days(1)),
                Some(PeriodToken::Week) => {
                    let wd = today.weekday().num_days_from_monday();
                    let start_of_week = today - chrono::Duration::days(wd as i64);
                    Ok(start_of_week + chrono::Duration::days(7))
                }
                Some(PeriodToken::Month) => {
                    let som = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap();
                    Ok(add_months_to_date(som, 1))
                }
                Some(PeriodToken::Quarter) => {
                    let qm = quarter_start_month(today.month());
                    let soq = NaiveDate::from_ymd_opt(today.year(), qm, 1).unwrap();
                    Ok(add_months_to_date(soq, 3))
                }
                Some(PeriodToken::Year) => Ok(NaiveDate::from_ymd_opt(today.year() + 1, 1, 1)
                    .ok_or_else(|| PeriodParseError::InvalidFormat("invalid year".into()))?),
                _ => Err(PeriodParseError::InvalidFormat(
                    "expected time unit or number after 'next'".into(),
                )),
            },
        }
    }

    /// "next N <units>" → since=today, until=today + N units
    fn resolve_next_n(&mut self, today: NaiveDate) -> Result<NaiveDate, PeriodParseError> {
        let n = match self.advance() {
            Some(PeriodToken::Number(n)) => *n,
            _ => unreachable!(),
        };
        // Return today; the caller will set since=today. We need to peek
        // and consume the unit to compute the end, but this method only
        // returns a single date. We'll handle "last/next N units" in
        // parse_relative_range instead.
        // Actually, when called from parse_date_expr, we just return the
        // offset date.
        match self.advance() {
            Some(PeriodToken::Day | PeriodToken::Days) => {
                Ok(today + chrono::Duration::days(n as i64))
            }
            Some(PeriodToken::Week | PeriodToken::Weeks) => {
                Ok(today + chrono::Duration::weeks(n as i64))
            }
            Some(PeriodToken::Month | PeriodToken::Months) => Ok(add_months_to_date(today, n)),
            Some(PeriodToken::Quarter | PeriodToken::Quarters) => {
                Ok(add_months_to_date(today, n * 3))
            }
            Some(PeriodToken::Year | PeriodToken::Years) => Ok(add_years_to_date(today, n)),
            _ => Err(PeriodParseError::InvalidFormat(
                "expected time unit after number".into(),
            )),
        }
    }

    /// "last" <unit> → start of previous period
    fn resolve_last(&mut self, today: NaiveDate) -> Result<NaiveDate, PeriodParseError> {
        match self.peek().cloned() {
            // "last N <unit>" → N units ago
            Some(PeriodToken::Number(_)) => self.resolve_last_n(today),
            _ => match self.advance() {
                Some(PeriodToken::Day) => Ok(today - chrono::Duration::days(1)),
                Some(PeriodToken::Week) => {
                    let wd = today.weekday().num_days_from_monday();
                    let start_of_week = today - chrono::Duration::days(wd as i64);
                    Ok(start_of_week - chrono::Duration::days(7))
                }
                Some(PeriodToken::Month) => {
                    let som = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap();
                    Ok(add_months_to_date(som, -1))
                }
                Some(PeriodToken::Quarter) => {
                    let qm = quarter_start_month(today.month());
                    let soq = NaiveDate::from_ymd_opt(today.year(), qm, 1).unwrap();
                    Ok(add_months_to_date(soq, -3))
                }
                Some(PeriodToken::Year) => Ok(NaiveDate::from_ymd_opt(today.year() - 1, 1, 1)
                    .ok_or_else(|| PeriodParseError::InvalidFormat("invalid year".into()))?),
                _ => Err(PeriodParseError::InvalidFormat(
                    "expected time unit or number after 'last'".into(),
                )),
            },
        }
    }

    /// "last N <units>" → since = today - N units, result = since date
    fn resolve_last_n(&mut self, today: NaiveDate) -> Result<NaiveDate, PeriodParseError> {
        let n = match self.advance() {
            Some(PeriodToken::Number(n)) => *n,
            _ => unreachable!(),
        };
        match self.advance() {
            Some(PeriodToken::Day | PeriodToken::Days) => {
                Ok(today - chrono::Duration::days(n as i64))
            }
            Some(PeriodToken::Week | PeriodToken::Weeks) => {
                Ok(today - chrono::Duration::weeks(n as i64))
            }
            Some(PeriodToken::Month | PeriodToken::Months) => Ok(add_months_to_date(today, -n)),
            Some(PeriodToken::Quarter | PeriodToken::Quarters) => {
                Ok(add_months_to_date(today, -n * 3))
            }
            Some(PeriodToken::Year | PeriodToken::Years) => Ok(add_years_to_date(today, -n)),
            _ => Err(PeriodParseError::InvalidFormat(
                "expected time unit after number".into(),
            )),
        }
    }

    /// N <unit> ago/hence
    fn resolve_relative_offset(
        &mut self,
        n: i32,
        today: NaiveDate,
    ) -> Result<NaiveDate, PeriodParseError> {
        let unit = self.advance().cloned();
        let direction = self.advance().cloned();

        let multiplier = match direction {
            Some(PeriodToken::Ago) => -1,
            Some(PeriodToken::Hence) => 1,
            _ => {
                return Err(PeriodParseError::InvalidFormat(
                    "expected 'ago' or 'hence' after time unit".into(),
                ))
            }
        };

        match unit {
            Some(PeriodToken::Day | PeriodToken::Days) => {
                Ok(today + chrono::Duration::days((n * multiplier) as i64))
            }
            Some(PeriodToken::Week | PeriodToken::Weeks) => {
                Ok(today + chrono::Duration::weeks((n * multiplier) as i64))
            }
            Some(PeriodToken::Month | PeriodToken::Months) => {
                Ok(add_months_to_date(today, n * multiplier))
            }
            Some(PeriodToken::Quarter | PeriodToken::Quarters) => {
                Ok(add_months_to_date(today, n * multiplier * 3))
            }
            Some(PeriodToken::Year | PeriodToken::Years) => {
                Ok(add_years_to_date(today, n * multiplier))
            }
            _ => Err(PeriodParseError::InvalidFormat(
                "expected time unit".into(),
            )),
        }
    }

    /// Handle "this/next/last <unit>" as a range (sets since+until on result).
    fn parse_relative_range(
        &mut self,
        result: &mut PeriodExpression,
    ) -> Result<(), PeriodParseError> {
        let modifier = self.advance().cloned();
        let today = current_date();

        // Check for "last/next N <units>" pattern
        if matches!(modifier, Some(PeriodToken::Last | PeriodToken::Next)) {
            if let Some(PeriodToken::Number(n)) = self.peek().cloned() {
                self.advance();
                return self.parse_relative_n_range(result, &modifier.unwrap(), n, today);
            }
        }

        let (start, end) = match self.advance() {
            Some(PeriodToken::Day) => {
                let d = match modifier.as_ref().unwrap() {
                    PeriodToken::This => today,
                    PeriodToken::Next => today + chrono::Duration::days(1),
                    PeriodToken::Last => today - chrono::Duration::days(1),
                    _ => unreachable!(),
                };
                (d, d + chrono::Duration::days(1))
            }
            Some(PeriodToken::Week) => {
                let wd = today.weekday().num_days_from_monday();
                let sow = today - chrono::Duration::days(wd as i64);
                match modifier.as_ref().unwrap() {
                    PeriodToken::This => (sow, sow + chrono::Duration::days(7)),
                    PeriodToken::Next => {
                        let s = sow + chrono::Duration::days(7);
                        (s, s + chrono::Duration::days(7))
                    }
                    PeriodToken::Last => {
                        let s = sow - chrono::Duration::days(7);
                        (s, sow)
                    }
                    _ => unreachable!(),
                }
            }
            Some(PeriodToken::Month) => {
                let som = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap();
                match modifier.as_ref().unwrap() {
                    PeriodToken::This => (som, add_months_to_date(som, 1)),
                    PeriodToken::Next => {
                        let s = add_months_to_date(som, 1);
                        (s, add_months_to_date(s, 1))
                    }
                    PeriodToken::Last => {
                        let s = add_months_to_date(som, -1);
                        (s, som)
                    }
                    _ => unreachable!(),
                }
            }
            Some(PeriodToken::Quarter) => {
                let qm = quarter_start_month(today.month());
                let soq = NaiveDate::from_ymd_opt(today.year(), qm, 1).unwrap();
                match modifier.as_ref().unwrap() {
                    PeriodToken::This => (soq, add_months_to_date(soq, 3)),
                    PeriodToken::Next => {
                        let s = add_months_to_date(soq, 3);
                        (s, add_months_to_date(s, 3))
                    }
                    PeriodToken::Last => {
                        let s = add_months_to_date(soq, -3);
                        (s, soq)
                    }
                    _ => unreachable!(),
                }
            }
            Some(PeriodToken::Year) => {
                let soy = NaiveDate::from_ymd_opt(today.year(), 1, 1).unwrap();
                match modifier.as_ref().unwrap() {
                    PeriodToken::This => {
                        (soy, NaiveDate::from_ymd_opt(today.year() + 1, 1, 1).unwrap())
                    }
                    PeriodToken::Next => {
                        let s = NaiveDate::from_ymd_opt(today.year() + 1, 1, 1).unwrap();
                        (s, NaiveDate::from_ymd_opt(today.year() + 2, 1, 1).unwrap())
                    }
                    PeriodToken::Last => {
                        let s = NaiveDate::from_ymd_opt(today.year() - 1, 1, 1).unwrap();
                        (s, soy)
                    }
                    _ => unreachable!(),
                }
            }
            _ => {
                return Err(PeriodParseError::InvalidFormat(
                    "expected time unit after this/next/last".into(),
                ))
            }
        };

        result.since = Some(start);
        result.until = Some(end);
        Ok(())
    }

    /// "last/next N <units>" → range
    fn parse_relative_n_range(
        &mut self,
        result: &mut PeriodExpression,
        modifier: &PeriodToken,
        n: i32,
        today: NaiveDate,
    ) -> Result<(), PeriodParseError> {
        let offset = match self.advance() {
            Some(PeriodToken::Day | PeriodToken::Days) => chrono::Duration::days(n as i64),
            Some(PeriodToken::Week | PeriodToken::Weeks) => chrono::Duration::weeks(n as i64),
            Some(PeriodToken::Month | PeriodToken::Months) => {
                let target = match modifier {
                    PeriodToken::Last => add_months_to_date(today, -n),
                    PeriodToken::Next => add_months_to_date(today, n),
                    _ => unreachable!(),
                };
                result.since = Some(if modifier == &PeriodToken::Last { target } else { today });
                result.until = Some(if modifier == &PeriodToken::Last { today } else { target });
                return Ok(());
            }
            Some(PeriodToken::Quarter | PeriodToken::Quarters) => {
                let target = match modifier {
                    PeriodToken::Last => add_months_to_date(today, -n * 3),
                    PeriodToken::Next => add_months_to_date(today, n * 3),
                    _ => unreachable!(),
                };
                result.since = Some(if modifier == &PeriodToken::Last { target } else { today });
                result.until = Some(if modifier == &PeriodToken::Last { today } else { target });
                return Ok(());
            }
            Some(PeriodToken::Year | PeriodToken::Years) => {
                let target = match modifier {
                    PeriodToken::Last => add_years_to_date(today, -n),
                    PeriodToken::Next => add_years_to_date(today, n),
                    _ => unreachable!(),
                };
                result.since = Some(if modifier == &PeriodToken::Last { target } else { today });
                result.until = Some(if modifier == &PeriodToken::Last { today } else { target });
                return Ok(());
            }
            _ => {
                return Err(PeriodParseError::InvalidFormat(
                    "expected time unit after number".into(),
                ))
            }
        };

        match modifier {
            PeriodToken::Last => {
                result.since = Some(today - offset);
                result.until = Some(today);
            }
            PeriodToken::Next => {
                result.since = Some(today);
                result.until = Some(today + offset);
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Return the first month of the quarter containing `month`.
fn quarter_start_month(month: u32) -> u32 {
    match month {
        1..=3 => 1,
        4..=6 => 4,
        7..=9 => 7,
        _ => 10,
    }
}

/// Convert a [`PeriodExpression`] into a [`DateInterval`].
pub fn period_expression_to_interval(expr: &PeriodExpression) -> DateInterval {
    DateInterval {
        start: expr.since,
        end: expr.until,
        period: expr.period.clone(),
        end_inclusive: expr.end_inclusive,
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datetime::{set_epoch, clear_epoch};
    use chrono::NaiveDate;

    /// Set a fixed "today" for deterministic tests.
    fn with_fixed_date(f: impl FnOnce()) {
        let dt = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        set_epoch(dt);
        f();
        clear_epoch();
    }

    // ── Tokenizer tests ──────────────────────────────────────────

    #[test]
    fn tokenize_simple_keywords() {
        let tokens = tokenize_period("daily");
        assert_eq!(tokens, vec![PeriodToken::Daily]);

        let tokens = tokenize_period("monthly");
        assert_eq!(tokens, vec![PeriodToken::Monthly]);
    }

    #[test]
    fn tokenize_every_n() {
        let tokens = tokenize_period("every 2 weeks");
        assert_eq!(
            tokens,
            vec![PeriodToken::Every, PeriodToken::Number(2), PeriodToken::Weeks]
        );
    }

    #[test]
    fn tokenize_date_literal() {
        let tokens = tokenize_period("from 2024/01/01 to 2024/12/31");
        assert_eq!(tokens.len(), 4);
        assert!(matches!(tokens[0], PeriodToken::Since));
        assert!(matches!(tokens[1], PeriodToken::DateLiteral(_)));
        assert!(matches!(tokens[2], PeriodToken::Until));
        assert!(matches!(tokens[3], PeriodToken::DateLiteral(_)));
    }

    #[test]
    fn tokenize_dash_range() {
        let tokens = tokenize_period("2024/01/01 - 2024/06/30");
        assert_eq!(tokens.len(), 3);
        assert!(matches!(tokens[0], PeriodToken::DateLiteral(_)));
        assert_eq!(tokens[1], PeriodToken::Dash);
        assert!(matches!(tokens[2], PeriodToken::DateLiteral(_)));
    }

    #[test]
    fn tokenize_relative() {
        let tokens = tokenize_period("last 3 months");
        assert_eq!(
            tokens,
            vec![PeriodToken::Last, PeriodToken::Number(3), PeriodToken::Months]
        );
    }

    #[test]
    fn tokenize_ago() {
        let tokens = tokenize_period("3 months ago");
        assert_eq!(
            tokens,
            vec![PeriodToken::Number(3), PeriodToken::Months, PeriodToken::Ago]
        );
    }

    #[test]
    fn tokenize_month_name() {
        let tokens = tokenize_period("February 2024");
        assert_eq!(
            tokens,
            vec![PeriodToken::February, PeriodToken::Number(2024)]
        );
    }

    #[test]
    fn tokenize_combined() {
        let tokens = tokenize_period("monthly from 2024/01/01 to 2024/12/31");
        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens[0], PeriodToken::Monthly);
        assert!(matches!(tokens[1], PeriodToken::Since));
    }

    // ── Parser tests (simple periods) ────────────────────────────

    #[test]
    fn parse_daily() {
        let tokens = tokenize_period("daily");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Daily(1)));
        assert_eq!(expr.since, None);
        assert_eq!(expr.until, None);
    }

    #[test]
    fn parse_weekly() {
        let tokens = tokenize_period("weekly");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Weekly(1)));
    }

    #[test]
    fn parse_monthly() {
        let tokens = tokenize_period("monthly");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Monthly(1)));
    }

    #[test]
    fn parse_quarterly() {
        let tokens = tokenize_period("quarterly");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Quarterly(1)));
    }

    #[test]
    fn parse_yearly() {
        let tokens = tokenize_period("yearly");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Yearly(1)));
    }

    #[test]
    fn parse_biweekly() {
        let tokens = tokenize_period("biweekly");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Biweekly));
    }

    #[test]
    fn parse_bimonthly() {
        let tokens = tokenize_period("bimonthly");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Bimonthly));
    }

    // ── Every N tests ────────────────────────────────────────────

    #[test]
    fn parse_every_2_weeks() {
        let tokens = tokenize_period("every 2 weeks");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Weekly(2)));
    }

    #[test]
    fn parse_every_3_months() {
        let tokens = tokenize_period("every 3 months");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Monthly(3)));
    }

    #[test]
    fn parse_every_day() {
        let tokens = tokenize_period("every day");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Daily(1)));
    }

    // ── Range tests ──────────────────────────────────────────────

    #[test]
    fn parse_from_to() {
        let tokens = tokenize_period("from 2024/01/01 to 2024/12/31");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(
            expr.since,
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            expr.until,
            Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
        );
        assert!(!expr.end_inclusive);
    }

    #[test]
    fn parse_since_until() {
        let tokens = tokenize_period("since 2024/01/01 until 2024/12/31");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(
            expr.since,
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            expr.until,
            Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
        );
    }

    #[test]
    fn parse_dash_range() {
        let tokens = tokenize_period("2024/01/01 - 2024/06/30");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(
            expr.since,
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            expr.until,
            Some(NaiveDate::from_ymd_opt(2024, 6, 30).unwrap())
        );
        assert!(expr.end_inclusive);
    }

    // ── Relative date tests ──────────────────────────────────────

    #[test]
    fn parse_this_month() {
        with_fixed_date(|| {
            let tokens = tokenize_period("this month");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap())
            );
            assert_eq!(
                expr.until,
                Some(NaiveDate::from_ymd_opt(2024, 7, 1).unwrap())
            );
        });
    }

    #[test]
    fn parse_this_year() {
        with_fixed_date(|| {
            let tokens = tokenize_period("this year");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
            );
            assert_eq!(
                expr.until,
                Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap())
            );
        });
    }

    #[test]
    fn parse_last_month() {
        with_fixed_date(|| {
            let tokens = tokenize_period("last month");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 5, 1).unwrap())
            );
            assert_eq!(
                expr.until,
                Some(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap())
            );
        });
    }

    #[test]
    fn parse_next_quarter() {
        with_fixed_date(|| {
            let tokens = tokenize_period("next quarter");
            let expr = parse_period_expression(&tokens).unwrap();
            // June is in Q2 (Apr-Jun), next quarter starts Jul
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 7, 1).unwrap())
            );
            assert_eq!(
                expr.until,
                Some(NaiveDate::from_ymd_opt(2024, 10, 1).unwrap())
            );
        });
    }

    // ── Ago / hence tests ────────────────────────────────────────

    #[test]
    fn parse_3_months_ago() {
        with_fixed_date(|| {
            let tokens = tokenize_period("since 3 months ago");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap())
            );
        });
    }

    #[test]
    fn parse_2_years_hence() {
        with_fixed_date(|| {
            let tokens = tokenize_period("until 2 years hence");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.until,
                Some(NaiveDate::from_ymd_opt(2026, 6, 15).unwrap())
            );
        });
    }

    // ── Last/next N units ────────────────────────────────────────

    #[test]
    fn parse_last_3_months_range() {
        with_fixed_date(|| {
            let tokens = tokenize_period("last 3 months");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap())
            );
            assert_eq!(
                expr.until,
                Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap())
            );
        });
    }

    #[test]
    fn parse_next_2_weeks_range() {
        with_fixed_date(|| {
            let tokens = tokenize_period("next 2 weeks");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap())
            );
            assert_eq!(
                expr.until,
                Some(NaiveDate::from_ymd_opt(2024, 6, 29).unwrap())
            );
        });
    }

    // ── Combined tests ───────────────────────────────────────────

    #[test]
    fn parse_monthly_from_to() {
        let tokens = tokenize_period("monthly from 2024/01/01 to 2024/12/31");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Monthly(1)));
        assert_eq!(
            expr.since,
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            expr.until,
            Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
        );
    }

    #[test]
    fn parse_weekly_since_last_month() {
        with_fixed_date(|| {
            let tokens = tokenize_period("weekly since last month");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(expr.period, Some(Period::Weekly(1)));
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 5, 1).unwrap())
            );
        });
    }

    // ── Month name tests ─────────────────────────────────────────

    #[test]
    fn parse_in_february() {
        with_fixed_date(|| {
            let tokens = tokenize_period("in February");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 2, 1).unwrap())
            );
            // "in February" → the month of February (Feb 1 to Mar 1)
            assert_eq!(
                expr.until,
                Some(NaiveDate::from_ymd_opt(2024, 3, 1).unwrap())
            );
        });
    }

    #[test]
    fn parse_february_2024() {
        let tokens = tokenize_period("February 2024");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(
            expr.since,
            Some(NaiveDate::from_ymd_opt(2024, 2, 1).unwrap())
        );
        assert_eq!(
            expr.until,
            Some(NaiveDate::from_ymd_opt(2024, 3, 1).unwrap())
        );
    }

    // ── Today / yesterday / tomorrow ─────────────────────────────

    #[test]
    fn parse_today() {
        with_fixed_date(|| {
            let tokens = tokenize_period("since today");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap())
            );
        });
    }

    #[test]
    fn parse_yesterday() {
        with_fixed_date(|| {
            let tokens = tokenize_period("since yesterday");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.since,
                Some(NaiveDate::from_ymd_opt(2024, 6, 14).unwrap())
            );
        });
    }

    #[test]
    fn parse_tomorrow() {
        with_fixed_date(|| {
            let tokens = tokenize_period("until tomorrow");
            let expr = parse_period_expression(&tokens).unwrap();
            assert_eq!(
                expr.until,
                Some(NaiveDate::from_ymd_opt(2024, 6, 16).unwrap())
            );
        });
    }

    // ── In YEAR ──────────────────────────────────────────────────

    #[test]
    fn parse_in_2024() {
        let tokens = tokenize_period("in 2024");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(
            expr.since,
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            expr.until,
            Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap())
        );
    }

    // ── Error cases ──────────────────────────────────────────────

    #[test]
    fn parse_empty_returns_empty_expression() {
        let tokens = tokenize_period("");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, None);
        assert_eq!(expr.since, None);
        assert_eq!(expr.until, None);
    }

    #[test]
    fn parse_unknown_token() {
        let tokens = tokenize_period("foobar");
        let result = parse_period_expression(&tokens);
        assert!(result.is_err());
    }

    // ── Conversion to DateInterval ───────────────────────────────

    #[test]
    fn period_expr_to_interval() {
        let tokens = tokenize_period("monthly from 2024/01/01 to 2024/12/31");
        let expr = parse_period_expression(&tokens).unwrap();
        let interval = period_expression_to_interval(&expr);
        assert_eq!(interval.period, Some(Period::Monthly(1)));
        assert_eq!(
            interval.start,
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            interval.end,
            Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
        );
    }

    // ── Partial date literal tests ───────────────────────────────

    #[test]
    fn parse_from_partial_date() {
        let tokens = tokenize_period("from 2024/01 to 2024/06");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(
            expr.since,
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            expr.until,
            Some(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap())
        );
    }

    #[test]
    fn parse_every_2_years() {
        let tokens = tokenize_period("every 2 years");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Yearly(2)));
    }

    #[test]
    fn parse_every_quarter() {
        let tokens = tokenize_period("every quarter");
        let expr = parse_period_expression(&tokens).unwrap();
        assert_eq!(expr.period, Some(Period::Quarterly(1)));
    }
}
