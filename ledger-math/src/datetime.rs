//! Date and time handling for Ledger
//!
//! This module provides date and time functionality compatible with the C++ Ledger
//! implementation, including timezone support, various date formats, epoch override
//! for deterministic testing, and period/interval types.

use chrono::{
    DateTime, Datelike, Days, Months, NaiveDate, NaiveDateTime, TimeDelta, TimeZone, Timelike, Utc,
};
use chrono_tz::{America, Tz};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::OnceLock;
use thiserror::Error;

/// Errors that can occur during date/time operations
#[derive(Error, Debug, Clone, PartialEq)]
pub enum DateTimeError {
    #[error("Invalid date format: {0}")]
    InvalidFormat(String),
    #[error("Date out of range: {0}")]
    OutOfRange(String),
    #[error("Timezone error: {0}")]
    TimezoneError(String),
    #[error("Ambiguous time during DST transition")]
    AmbiguousTime,
}

pub type DateTimeResult<T> = Result<T, DateTimeError>;

/// Default timezone for Ledger (America/Chicago for test compatibility)
pub static DEFAULT_TIMEZONE: Tz = America::Chicago;

// ---------------------------------------------------------------------------
// Global epoch override (mirrors C++ `optional<datetime_t> epoch`)
// ---------------------------------------------------------------------------

/// Global epoch override. When set, `current_date()` and `current_datetime()`
/// return values derived from the epoch instead of the wall clock. This is used
/// by the `--now` CLI flag and for deterministic tests.
static EPOCH: OnceLock<RwLock<Option<NaiveDateTime>>> = OnceLock::new();

/// Set the global epoch to the given datetime.
pub fn set_epoch(dt: NaiveDateTime) {
    let lock = EPOCH.get_or_init(|| RwLock::new(None));
    *lock.write() = Some(dt);
}

/// Clear the global epoch, reverting to wall-clock time.
pub fn clear_epoch() {
    if let Some(lock) = EPOCH.get() {
        *lock.write() = None;
    }
}

/// Return the current date, respecting the global epoch if set.
pub fn current_date() -> NaiveDate {
    if let Some(lock) = EPOCH.get() {
        if let Some(epoch) = *lock.read() {
            return epoch.date();
        }
    }
    chrono::Local::now().naive_local().date()
}

/// Return the current datetime, respecting the global epoch if set.
pub fn current_datetime() -> NaiveDateTime {
    if let Some(lock) = EPOCH.get() {
        if let Some(epoch) = *lock.read() {
            return epoch;
        }
    }
    chrono::Local::now().naive_local()
}

/// Date type compatible with C++ date_t
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Date(pub NaiveDate);

impl Date {
    /// Create a new Date
    pub fn new(year: i32, month: u32, day: u32) -> DateTimeResult<Self> {
        NaiveDate::from_ymd_opt(year, month, day)
            .map(Date)
            .ok_or_else(|| DateTimeError::OutOfRange(format!("{}-{:02}-{:02}", year, month, day)))
    }

    /// Create a Date from a NaiveDate
    pub fn from_naive_date(date: NaiveDate) -> Self {
        Date(date)
    }

    /// Get the current date, respecting the global epoch if set.
    pub fn current() -> Self {
        Date(current_date())
    }

    /// Check if this is a valid date (always true for constructed dates)
    pub fn is_valid(&self) -> bool {
        true
    }

    /// Check if this is not a valid date (always false for constructed dates)
    pub fn is_not_a_date(&self) -> bool {
        false
    }

    /// Get the underlying NaiveDate
    pub fn naive_date(&self) -> NaiveDate {
        self.0
    }

    /// Get year
    pub fn year(&self) -> i32 {
        self.0.year()
    }

    /// Get month (1-12)
    pub fn month(&self) -> u32 {
        self.0.month()
    }

    /// Get day of month (1-31)
    pub fn day(&self) -> u32 {
        self.0.day()
    }

    /// Format the date using a format string
    pub fn format<'a>(&self, fmt: &'a str) -> impl fmt::Display + 'a {
        self.0.format(fmt)
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.format("%Y-%m-%d"))
    }
}

// Arithmetic operations for Date
impl std::ops::Add<TimeDelta> for Date {
    type Output = Date;

    fn add(self, rhs: TimeDelta) -> Self::Output {
        Date(self.0 + rhs)
    }
}

impl std::ops::Sub<TimeDelta> for Date {
    type Output = Date;

    fn sub(self, rhs: TimeDelta) -> Self::Output {
        Date(self.0 - rhs)
    }
}

impl std::ops::Sub<Date> for Date {
    type Output = TimeDelta;

    fn sub(self, rhs: Date) -> Self::Output {
        self.0 - rhs.0
    }
}

impl std::ops::Sub<Date> for NaiveDate {
    type Output = TimeDelta;

    fn sub(self, rhs: Date) -> Self::Output {
        self - rhs.0
    }
}

/// DateTime type compatible with C++ datetime_t
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalDateTime {
    datetime: DateTime<Tz>,
}

impl LocalDateTime {
    /// Create a new LocalDateTime in the default timezone
    pub fn new(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
    ) -> DateTimeResult<Self> {
        let naive_dt = NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|d| d.and_hms_opt(hour, min, sec))
            .ok_or_else(|| {
                DateTimeError::OutOfRange(format!(
                    "{}-{:02}-{:02} {:02}:{:02}:{:02}",
                    year, month, day, hour, min, sec
                ))
            })?;

        let datetime = DEFAULT_TIMEZONE
            .from_local_datetime(&naive_dt)
            .single()
            .ok_or(DateTimeError::AmbiguousTime)?;

        Ok(LocalDateTime { datetime })
    }

    /// Create from a DateTime with timezone
    pub fn from_datetime(datetime: DateTime<Tz>) -> Self {
        LocalDateTime { datetime }
    }

    /// Get the current time, respecting the global epoch if set.
    pub fn current() -> Self {
        let naive = current_datetime();
        let datetime = DEFAULT_TIMEZONE
            .from_local_datetime(&naive)
            .single()
            .unwrap_or_else(|| Utc::now().with_timezone(&DEFAULT_TIMEZONE));
        LocalDateTime { datetime }
    }

    /// Convert to UTC
    pub fn to_utc(&self) -> DateTime<Utc> {
        self.datetime.with_timezone(&Utc)
    }

    /// Convert to a different timezone
    pub fn with_timezone(&self, tz: Tz) -> LocalDateTime {
        LocalDateTime { datetime: self.datetime.with_timezone(&tz) }
    }

    /// Get the date part
    pub fn date(&self) -> Date {
        Date(self.datetime.date_naive())
    }

    /// Get the timezone
    pub fn timezone(&self) -> Tz {
        self.datetime.timezone()
    }

    /// Check if this is a valid datetime
    pub fn is_valid(&self) -> bool {
        true
    }

    /// Check if this is not a valid datetime
    pub fn is_not_a_date_time(&self) -> bool {
        false
    }

    /// Get the hour component (0-23)
    pub fn hour(&self) -> u32 {
        self.datetime.hour()
    }

    /// Get the minute component (0-59)
    pub fn minute(&self) -> u32 {
        self.datetime.minute()
    }

    /// Get the second component (0-59)
    pub fn second(&self) -> u32 {
        self.datetime.second()
    }
}

impl fmt::Display for LocalDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.datetime.format("%Y-%m-%d %H:%M:%S %Z"))
    }
}

/// Format type for date/datetime formatting
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FormatType {
    Written,
    Printed,
    Custom,
}

/// Date parsing function compatible with C++ parse_date
pub fn parse_date(date_str: &str) -> DateTimeResult<Date> {
    let trimmed = date_str.trim();

    // Try various date formats in order of preference
    let formats = [
        "%Y/%m/%d", // 1990/01/01
        "%Y-%m-%d", // 2006-12-25
        "%Y.%m.%d", // 2006.12.25
        "%m/%d/%Y", // 02/02/2002 (US format)
        "%m-%d-%Y", // 02-02-2002 (US format)
        "%m.%d.%Y", // 02.02.2002 (US format)
        "%y/%m/%d", // 02/02/02 (two digit year)
        "%m/%d/%y", // 02/02/02 (US format, two digit year)
        "%m-%d-%y", // 02-02-02 (US format, two digit year)
        "%m.%d.%y", // 02.02.02 (US format, two digit year)
        "%Y%m%d",   // 20020202 (compact format)
        "%m/%d",    // 12/25 (current year assumed)
        "%m-%d",    // 12-25 (current year assumed)
        "%m.%d",    // 12.25 (current year assumed)
        "%Y/%m",    // 1990/01 (day 1 assumed)
        "%Y-%m",    // 1990-01 (day 1 assumed)
        "%Y.%m",    // 1990.01 (day 1 assumed)
        "%Y",       // 1990 (Jan 1 assumed)
    ];

    // Get current year for formats that don't specify year
    let current_year = Date::current().year();

    for format in &formats {
        if let Ok(date) = parse_date_with_format(trimmed, format, current_year) {
            return Ok(date);
        }
    }

    // Try parsing month and day names (simplified for now)
    if let Ok(date) = parse_relative_date(trimmed) {
        return Ok(date);
    }

    Err(DateTimeError::InvalidFormat(date_str.to_string()))
}

fn parse_date_with_format(date_str: &str, format: &str, current_year: i32) -> DateTimeResult<Date> {
    // Handle formats without year by adding current year
    let (actual_str, actual_format) = match format {
        "%m/%d" => (format!("{}/{}", current_year, date_str), "%Y/%m/%d".to_string()),
        "%m-%d" => (format!("{}-{}", current_year, date_str), "%Y-%m-%d".to_string()),
        "%m.%d" => (format!("{}.{}", current_year, date_str), "%Y.%m.%d".to_string()),
        "%Y/%m" => (format!("{}/01", date_str), "%Y/%m/%d".to_string()),
        "%Y-%m" => (format!("{}-01", date_str), "%Y-%m-%d".to_string()),
        "%Y.%m" => (format!("{}.01", date_str), "%Y.%m.%d".to_string()),
        "%Y" => (format!("{}/01/01", date_str), "%Y/%m/%d".to_string()),
        _ => (date_str.to_string(), format.to_string()),
    };

    NaiveDate::parse_from_str(&actual_str, &actual_format)
        .map(Date)
        .map_err(|_| DateTimeError::InvalidFormat(date_str.to_string()))
}

fn parse_relative_date(date_str: &str) -> DateTimeResult<Date> {
    let lower = date_str.to_lowercase();
    let current = Date::current();

    // Handle simple cases for now
    match lower.as_str() {
        "today" => Ok(current),
        "yesterday" => Ok(Date::from_naive_date(current.naive_date() - chrono::Duration::days(1))),
        "tomorrow" => Ok(Date::from_naive_date(current.naive_date() + chrono::Duration::days(1))),
        _ => Err(DateTimeError::InvalidFormat(date_str.to_string())),
    }
}

/// DateTime parsing function compatible with C++ parse_datetime
pub fn parse_datetime(datetime_str: &str) -> DateTimeResult<LocalDateTime> {
    let trimmed = datetime_str.trim();

    // Try various datetime formats
    let formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y.%m.%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y.%m.%d %H:%M",
    ];

    for format in &formats {
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(trimmed, format) {
            let datetime = DEFAULT_TIMEZONE
                .from_local_datetime(&naive_dt)
                .single()
                .ok_or(DateTimeError::AmbiguousTime)?;
            return Ok(LocalDateTime::from_datetime(datetime));
        }
    }

    Err(DateTimeError::InvalidFormat(datetime_str.to_string()))
}

/// Format a date according to the specified format type.
///
/// C++ defaults: Written = `%Y/%m/%d`, Printed = `%y-%b-%d`
pub fn format_date(date: &Date, format_type: FormatType, custom_format: Option<&str>) -> String {
    match format_type {
        FormatType::Written => date.naive_date().format("%Y/%m/%d").to_string(),
        FormatType::Printed => date.naive_date().format("%y-%b-%d").to_string(),
        FormatType::Custom => {
            if let Some(fmt) = custom_format {
                date.naive_date().format(fmt).to_string()
            } else {
                date.naive_date().format("%Y/%m/%d").to_string()
            }
        }
    }
}

/// Format a datetime according to the specified format type.
///
/// C++ defaults: Written = `%Y/%m/%d %H:%M:%S`, Printed = `%y-%b-%d %H:%M:%S`
pub fn format_datetime(
    datetime: &LocalDateTime,
    format_type: FormatType,
    custom_format: Option<&str>,
) -> String {
    match format_type {
        FormatType::Written => datetime.datetime.format("%Y/%m/%d %H:%M:%S").to_string(),
        FormatType::Printed => datetime.datetime.format("%y-%b-%d %H:%M:%S").to_string(),
        FormatType::Custom => {
            if let Some(fmt) = custom_format {
                datetime.datetime.format(fmt).to_string()
            } else {
                datetime.datetime.format("%Y/%m/%d %H:%M:%S").to_string()
            }
        }
    }
}

/// Duration for date arithmetic
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateDuration {
    Days(i32),
    Weeks(i32),
    Months(i32),
    Quarters(i32),
    Years(i32),
}

impl DateDuration {
    /// Add this duration to a date
    pub fn add_to_date(&self, date: Date) -> Date {
        match *self {
            DateDuration::Days(n) if n >= 0 => Date(date.naive_date() + Days::new(n as u64)),
            DateDuration::Days(n) => Date(date.naive_date() - Days::new((-n) as u64)),
            DateDuration::Weeks(n) if n >= 0 => Date(date.naive_date() + Days::new((n * 7) as u64)),
            DateDuration::Weeks(n) => Date(date.naive_date() - Days::new(((-n) * 7) as u64)),
            DateDuration::Months(n) if n >= 0 => Date(date.naive_date() + Months::new(n as u32)),
            DateDuration::Months(n) => Date(date.naive_date() - Months::new((-n) as u32)),
            DateDuration::Quarters(n) if n >= 0 => {
                Date(date.naive_date() + Months::new((n * 3) as u32))
            }
            DateDuration::Quarters(n) => Date(date.naive_date() - Months::new(((-n) * 3) as u32)),
            DateDuration::Years(n) if n >= 0 => {
                Date(date.naive_date() + Months::new((n * 12) as u32))
            }
            DateDuration::Years(n) => Date(date.naive_date() - Months::new(((-n) * 12) as u32)),
        }
    }
}

// ---------------------------------------------------------------------------
// Period types for recurring intervals (merged from ledger-core)
// ---------------------------------------------------------------------------

/// Period types for recurring intervals
#[derive(Debug, Clone, PartialEq)]
pub enum Period {
    Daily(u32),
    Weekly(u32),
    Biweekly,
    Monthly(u32),
    Bimonthly,
    Quarterly(u32),
    Yearly(u32),
}

impl Period {
    /// Get the approximate number of days in the period
    pub fn approximate_days(&self) -> u32 {
        match self {
            Period::Daily(n) => *n,
            Period::Weekly(n) => n * 7,
            Period::Biweekly => 14,
            Period::Monthly(n) => n * 30,
            Period::Bimonthly => 60,
            Period::Quarterly(n) => n * 90,
            Period::Yearly(n) => n * 365,
        }
    }

    /// Get the period name as a string
    pub fn name(&self) -> String {
        match self {
            Period::Daily(1) => "daily".to_string(),
            Period::Daily(n) => format!("every {} days", n),
            Period::Weekly(1) => "weekly".to_string(),
            Period::Weekly(n) => format!("every {} weeks", n),
            Period::Biweekly => "biweekly".to_string(),
            Period::Monthly(1) => "monthly".to_string(),
            Period::Monthly(n) => format!("every {} months", n),
            Period::Bimonthly => "bimonthly".to_string(),
            Period::Quarterly(1) => "quarterly".to_string(),
            Period::Quarterly(n) => format!("every {} quarters", n),
            Period::Yearly(1) => "yearly".to_string(),
            Period::Yearly(n) => format!("every {} years", n),
        }
    }

    /// Add this period to a NaiveDate
    pub fn add_to_naive_date(&self, date: NaiveDate) -> NaiveDate {
        match self {
            Period::Daily(n) => date + chrono::Duration::days(*n as i64),
            Period::Weekly(n) => date + chrono::Duration::weeks(*n as i64),
            Period::Biweekly => date + chrono::Duration::weeks(2),
            Period::Monthly(n) => add_months_to_date(date, *n as i32),
            Period::Bimonthly => add_months_to_date(date, 2),
            Period::Quarterly(n) => add_months_to_date(date, (*n * 3) as i32),
            Period::Yearly(n) => add_years_to_date(date, *n as i32),
        }
    }

    /// Subtract this period from a NaiveDate
    pub fn subtract_from_naive_date(&self, date: NaiveDate) -> NaiveDate {
        match self {
            Period::Daily(n) => date - chrono::Duration::days(*n as i64),
            Period::Weekly(n) => date - chrono::Duration::weeks(*n as i64),
            Period::Biweekly => date - chrono::Duration::weeks(2),
            Period::Monthly(n) => add_months_to_date(date, -(*n as i32)),
            Period::Bimonthly => add_months_to_date(date, -2),
            Period::Quarterly(n) => add_months_to_date(date, -((*n * 3) as i32)),
            Period::Yearly(n) => add_years_to_date(date, -(*n as i32)),
        }
    }
}

// ---------------------------------------------------------------------------
// DateInterval with contains/overlaps/iter (merged from ledger-core)
// ---------------------------------------------------------------------------

/// A date interval with start, end, and period
#[derive(Debug, Clone)]
pub struct DateInterval {
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
    pub period: Option<Period>,
    pub end_inclusive: bool,
}

impl DateInterval {
    pub fn new(start: Option<NaiveDate>, end: Option<NaiveDate>, period: Option<Period>) -> Self {
        Self { start, end, period, end_inclusive: false }
    }

    pub fn from_period(period: Period) -> Self {
        Self { start: None, end: None, period: Some(period), end_inclusive: false }
    }

    pub fn from_range(start: NaiveDate, end: NaiveDate, inclusive: bool) -> Self {
        Self { start: Some(start), end: Some(end), period: None, end_inclusive: inclusive }
    }

    /// Check if a date falls within this interval
    pub fn contains(&self, date: NaiveDate) -> bool {
        let after_start = self.start.is_none_or(|s| date >= s);
        let before_end = match self.end {
            Some(end) if self.end_inclusive => date <= end,
            Some(end) => date < end,
            None => true,
        };
        after_start && before_end
    }

    /// Check if this interval overlaps with another
    pub fn overlaps(&self, other: &DateInterval) -> bool {
        let self_start = self.start.unwrap_or(NaiveDate::MIN);
        let self_end = self.effective_end();
        let other_start = other.start.unwrap_or(NaiveDate::MIN);
        let other_end = other.effective_end();
        self_start < other_end && other_start < self_end
    }

    /// Get the duration of this interval in days (if bounded)
    pub fn duration_days(&self) -> Option<i64> {
        match (self.start, self.end) {
            (Some(start), Some(_end)) => Some((self.effective_end() - start).num_days()),
            _ => None,
        }
    }

    /// Get the next date in the period after the given date
    pub fn next_date_after(&self, date: NaiveDate) -> Option<NaiveDate> {
        let period = self.period.as_ref()?;
        let next = period.add_to_naive_date(date);
        if self.contains(next) { Some(next) } else { None }
    }

    /// Create an iterator over dates in this interval
    pub fn iter_dates(&self) -> DateIntervalIterator {
        DateIntervalIterator { current: self.start, interval: self.clone(), done: false }
    }

    fn effective_end(&self) -> NaiveDate {
        let end = self.end.unwrap_or(NaiveDate::MAX);
        if self.end_inclusive && self.end.is_some() {
            end + chrono::Duration::days(1)
        } else {
            end
        }
    }
}

/// Iterator over dates in a DateInterval
pub struct DateIntervalIterator {
    interval: DateInterval,
    current: Option<NaiveDate>,
    done: bool,
}

impl Iterator for DateIntervalIterator {
    type Item = NaiveDate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let current = self.current?;
        if !self.interval.contains(current) {
            self.done = true;
            return None;
        }
        let result = current;
        if let Some(period) = &self.interval.period {
            self.current = Some(period.add_to_naive_date(current));
        } else {
            self.done = true;
        }
        Some(result)
    }
}

// ---------------------------------------------------------------------------
// DateDurationCompound (year+month+day compound duration, from ledger-core)
// ---------------------------------------------------------------------------

/// Compound duration with years, months, and days for relative date calculations
#[derive(Debug, Clone, PartialEq)]
pub struct DateDurationCompound {
    pub years: i32,
    pub months: i32,
    pub days: i64,
}

impl DateDurationCompound {
    pub fn new(years: i32, months: i32, days: i64) -> Self {
        Self { years, months, days }
    }

    pub fn from_days(days: i64) -> Self {
        Self { years: 0, months: 0, days }
    }

    pub fn from_months(months: i32) -> Self {
        Self { years: 0, months, days: 0 }
    }

    pub fn from_years(years: i32) -> Self {
        Self { years, months: 0, days: 0 }
    }

    /// Add this duration to a NaiveDate
    pub fn add_to_date(&self, date: NaiveDate) -> NaiveDate {
        let mut result = date;
        if self.years != 0 {
            result = add_years_to_date(result, self.years);
        }
        if self.months != 0 {
            result = add_months_to_date(result, self.months);
        }
        if self.days != 0 {
            result += chrono::Duration::days(self.days);
        }
        result
    }

    /// Subtract this duration from a NaiveDate
    pub fn subtract_from_date(&self, date: NaiveDate) -> NaiveDate {
        let mut result = date;
        if self.days != 0 {
            result -= chrono::Duration::days(self.days);
        }
        if self.months != 0 {
            result = add_months_to_date(result, -self.months);
        }
        if self.years != 0 {
            result = add_years_to_date(result, -self.years);
        }
        result
    }

    pub fn approximate_days(&self) -> i64 {
        (self.years as i64 * 365) + (self.months as i64 * 30) + self.days
    }

    pub fn is_zero(&self) -> bool {
        self.years == 0 && self.months == 0 && self.days == 0
    }
}

impl std::ops::Add for DateDurationCompound {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            years: self.years + other.years,
            months: self.months + other.months,
            days: self.days + other.days,
        }
    }
}

impl std::ops::Sub for DateDurationCompound {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        Self {
            years: self.years - other.years,
            months: self.months - other.months,
            days: self.days - other.days,
        }
    }
}

// ---------------------------------------------------------------------------
// Date arithmetic helpers (from ledger-core)
// ---------------------------------------------------------------------------

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => if is_leap_year(year) { 29 } else { 28 },
        _ => 30,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

pub fn add_months_to_date(date: NaiveDate, months: i32) -> NaiveDate {
    let mut new_year = date.year();
    let mut new_month = date.month() as i32 + months;
    while new_month > 12 {
        new_year += 1;
        new_month -= 12;
    }
    while new_month < 1 {
        new_year -= 1;
        new_month += 12;
    }
    let new_day = std::cmp::min(date.day(), days_in_month(new_year, new_month as u32));
    NaiveDate::from_ymd_opt(new_year, new_month as u32, new_day).unwrap_or(date)
}

pub fn add_years_to_date(date: NaiveDate, years: i32) -> NaiveDate {
    let new_year = date.year() + years;
    let new_day = if date.month() == 2 && date.day() == 29 && !is_leap_year(new_year) {
        28
    } else {
        date.day()
    };
    NaiveDate::from_ymd_opt(new_year, date.month(), new_day).unwrap_or(date)
}

// ---------------------------------------------------------------------------
// Period parsing (merged from ledger-core)
// ---------------------------------------------------------------------------

/// Errors that can occur during period parsing
#[derive(Error, Debug, PartialEq)]
pub enum PeriodParseError {
    #[error("Invalid period format: {0}")]
    InvalidFormat(String),
    #[error("Unknown period keyword: {0}")]
    UnknownKeyword(String),
    #[error("Invalid number in period: {0}")]
    InvalidNumber(String),
    #[error("Missing period specification")]
    MissingPeriod,
}

/// Parse a period expression string into a DateInterval.
///
/// Supports the full period expression grammar including:
/// - Simple periods: "daily", "weekly", "monthly", "quarterly", "yearly"
/// - Every N: "every 2 weeks", "every 3 months"
/// - Date ranges: "from 2024/01/01 to 2024/12/31"
/// - Relative: "this month", "last year", "next quarter"
/// - Ago/hence: "3 months ago", "2 years hence"
/// - Dash ranges: "2024/01 - 2024/06"
/// - Month names: "in February", "February 2024"
/// - Combined: "monthly from 2024/01 to 2024/12"
pub fn parse_period(input: &str) -> Result<DateInterval, PeriodParseError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(PeriodParseError::MissingPeriod);
    }

    let tokens = crate::period_parser::tokenize_period(input);
    let expr = crate::period_parser::parse_period_expression(&tokens)?;
    Ok(crate::period_parser::period_expression_to_interval(&expr))
}

/// Timezone utilities
pub mod timezone {
    use super::*;
    use std::str::FromStr;

    /// Parse a timezone from a string
    pub fn parse_timezone(tz_str: &str) -> DateTimeResult<Tz> {
        // Handle common timezone abbreviations
        let normalized = match tz_str.to_uppercase().as_str() {
            "EST" => "America/New_York",
            "CST" => "America/Chicago",
            "MST" => "America/Denver",
            "PST" => "America/Los_Angeles",
            "UTC" | "GMT" => "UTC",
            _ => tz_str,
        };

        Tz::from_str(normalized).map_err(|_| DateTimeError::TimezoneError(tz_str.to_string()))
    }

    /// Convert a datetime to a different timezone
    pub fn convert_timezone(dt: &LocalDateTime, target_tz: Tz) -> LocalDateTime {
        dt.with_timezone(target_tz)
    }

    /// Get the default timezone
    pub fn default_timezone() -> Tz {
        DEFAULT_TIMEZONE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_creation() {
        let date = Date::new(2023, 12, 25).unwrap();
        assert_eq!(date.year(), 2023);
        assert_eq!(date.month(), 12);
        assert_eq!(date.day(), 25);
        assert!(date.is_valid());
        assert!(!date.is_not_a_date());
    }

    #[test]
    fn test_parse_date_basic() {
        let date1 = parse_date("1990/01/01").unwrap();
        assert_eq!(date1.year(), 1990);
        assert_eq!(date1.month(), 1);
        assert_eq!(date1.day(), 1);

        let date2 = parse_date("2006-12-25").unwrap();
        assert_eq!(date2.year(), 2006);
        assert_eq!(date2.month(), 12);
        assert_eq!(date2.day(), 25);

        let date3 = parse_date("2006.12.25").unwrap();
        assert_eq!(date3, date2);
    }

    #[test]
    fn test_format_date() {
        let date = Date::new(2023, 12, 25).unwrap();
        // C++ Written = %Y/%m/%d, Printed = %y-%b-%d
        assert_eq!(format_date(&date, FormatType::Written, None), "2023/12/25");
        assert_eq!(format_date(&date, FormatType::Printed, None), "23-Dec-25");
    }

    #[test]
    fn test_current_date() {
        let current = Date::current();
        assert!(current.is_valid());
    }

    #[test]
    fn test_timezone_parsing() {
        assert!(timezone::parse_timezone("America/Chicago").is_ok());
        assert!(timezone::parse_timezone("EST").is_ok());
        assert!(timezone::parse_timezone("UTC").is_ok());
        assert!(timezone::parse_timezone("Invalid/Timezone").is_err());
    }

    #[test]
    fn test_epoch_set_and_clear() {
        let epoch_dt = NaiveDate::from_ymd_opt(2020, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();

        set_epoch(epoch_dt);
        assert_eq!(current_date(), NaiveDate::from_ymd_opt(2020, 6, 15).unwrap());
        assert_eq!(current_datetime(), epoch_dt);

        // Date::current() should also respect epoch
        let d = Date::current();
        assert_eq!(d.year(), 2020);
        assert_eq!(d.month(), 6);
        assert_eq!(d.day(), 15);

        clear_epoch();
        // After clearing, current_date should return today (not 2020)
        let now = current_date();
        assert_ne!(now, NaiveDate::from_ymd_opt(2020, 6, 15).unwrap());
    }

    #[test]
    fn test_period_basic() {
        assert_eq!(Period::Daily(1).name(), "daily");
        assert_eq!(Period::Weekly(2).name(), "every 2 weeks");
        assert_eq!(Period::Quarterly(1).name(), "quarterly");
    }

    #[test]
    fn test_period_parsing() {
        let daily = parse_period("daily").unwrap();
        assert_eq!(daily.period, Some(Period::Daily(1)));

        let every_3_days = parse_period("every 3 days").unwrap();
        assert_eq!(every_3_days.period, Some(Period::Daily(3)));
    }

    #[test]
    fn test_date_interval_contains() {
        let start = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();
        let interval = DateInterval::from_range(start, end, false);

        assert!(interval.contains(NaiveDate::from_ymd_opt(2023, 6, 15).unwrap()));
        assert!(interval.contains(start));
        assert!(!interval.contains(end)); // exclusive
    }

    #[test]
    fn test_date_interval_iterator() {
        let start = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2023, 1, 8).unwrap();
        let interval = DateInterval {
            start: Some(start),
            end: Some(end),
            period: Some(Period::Daily(2)),
            end_inclusive: false,
        };
        let dates: Vec<_> = interval.iter_dates().collect();
        assert_eq!(dates.len(), 4); // 1, 3, 5, 7
        assert_eq!(dates[0], NaiveDate::from_ymd_opt(2023, 1, 1).unwrap());
        assert_eq!(dates[3], NaiveDate::from_ymd_opt(2023, 1, 7).unwrap());
    }
}
