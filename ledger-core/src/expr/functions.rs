//! Built-in function implementations
//!
//! This module implements all the built-in functions available in expressions,
//! including mathematical, string, date/time, and utility functions.

use crate::expr::{BuiltinFunction, ExprContext, ExprError, ExprNode, ExprResult, Value};
use chrono::{Local, NaiveDate, TimeZone};
use ledger_math::{Amount, Balance, BigRational, KeepDetails};
use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use rust_decimal::{prelude::*, Decimal};

/// Evaluate a built-in function call
pub fn evaluate_builtin_function(
    function: BuiltinFunction,
    args: &[ExprNode],
    context: &ExprContext,
) -> ExprResult<Value> {
    // Evaluate all arguments first
    let mut evaluated_args = Vec::new();
    for arg in args {
        evaluated_args.push(super::evaluate_node(arg, context)?);
    }

    dispatch_builtin(function, &evaluated_args)
}

/// Evaluate a built-in function with pre-evaluated argument values.
///
/// This is used by the scope-based evaluator which evaluates arguments
/// itself before calling into the function dispatch.
pub fn evaluate_builtin_function_with_values(
    function: BuiltinFunction,
    args: &[Value],
) -> ExprResult<Value> {
    dispatch_builtin(function, args)
}

/// Central dispatch for all built-in functions.
fn dispatch_builtin(function: BuiltinFunction, args: &[Value]) -> ExprResult<Value> {
    match function {
        // Mathematical functions
        BuiltinFunction::Abs => fn_abs(args),
        BuiltinFunction::Floor => fn_floor(args),
        BuiltinFunction::Ceiling => fn_ceiling(args),
        BuiltinFunction::Round => fn_round(args),
        BuiltinFunction::Truncate => fn_truncate(args),
        BuiltinFunction::Min => fn_min(args),
        BuiltinFunction::Max => fn_max(args),

        // Date/time functions
        BuiltinFunction::Now => fn_now(args),
        BuiltinFunction::Today => fn_today(args),
        BuiltinFunction::Age => fn_age(args),
        BuiltinFunction::FormatDate => fn_format_date(args),

        // String functions
        BuiltinFunction::FormatString => fn_format_string(args),
        BuiltinFunction::ToUpper => fn_to_upper(args),
        BuiltinFunction::ToLower => fn_to_lower(args),
        BuiltinFunction::Trim => fn_trim(args),

        // Type conversion functions
        BuiltinFunction::ToString => fn_to_string(args),
        BuiltinFunction::ToInt => fn_to_int(args),
        BuiltinFunction::ToDecimal => fn_to_decimal(args),
        BuiltinFunction::ToAmount => fn_to_amount(args),
        BuiltinFunction::ToBoolean => fn_to_boolean(args),
        BuiltinFunction::ToDate => fn_to_date(args),
        BuiltinFunction::ToDatetime => fn_to_datetime(args),
        BuiltinFunction::ToBalance => fn_to_balance(args),

        // Aggregation functions
        BuiltinFunction::Sum => fn_sum(args),
        BuiltinFunction::Count => fn_count(args),
        BuiltinFunction::Average => fn_average(args),

        // Utility functions
        BuiltinFunction::IsEmpty => fn_is_empty(args),
        BuiltinFunction::Length => fn_length(args),
        BuiltinFunction::Type => fn_type(args),

        // Amount/Value functions
        BuiltinFunction::Quantity => fn_quantity(args),
        BuiltinFunction::Commodity => fn_commodity(args),
        BuiltinFunction::Rounded => fn_rounded(args),
        BuiltinFunction::Unrounded => fn_unrounded(args),
        BuiltinFunction::Truncated => fn_truncated(args),
        BuiltinFunction::Strip => fn_strip(args),
        BuiltinFunction::Scrub => fn_scrub(args),
        BuiltinFunction::Market => fn_market(args),
        BuiltinFunction::Exchange => fn_exchange(args),

        // Display functions
        BuiltinFunction::DisplayAmount => fn_display_amount(args),
        BuiltinFunction::DisplayTotal => fn_display_total(args),
        BuiltinFunction::Justify => fn_justify(args),
        BuiltinFunction::Quoted => fn_quoted(args),
        BuiltinFunction::QuotedRfc => fn_quoted_rfc(args),
        BuiltinFunction::AnsifyIf => fn_ansify_if(args),
        BuiltinFunction::ShouldBold => fn_should_bold(args),

        // Utility (additional)
        BuiltinFunction::Percent => fn_percent(args),
        BuiltinFunction::Join => fn_join(args),
        BuiltinFunction::GetAt => fn_get_at(args),
        BuiltinFunction::IsSeq => fn_is_seq(args),

        // Lot/Annotation functions
        BuiltinFunction::LotDate => fn_lot_date(args),
        BuiltinFunction::LotPrice => fn_lot_price(args),
        BuiltinFunction::LotTag => fn_lot_tag(args),
    }
}

/// Helper function to check argument count
fn check_arg_count(args: &[Value], expected: usize, function_name: &str) -> ExprResult<()> {
    if args.len() != expected {
        Err(ExprError::InvalidArgCount {
            function: function_name.to_string(),
            expected,
            found: args.len(),
        })
    } else {
        Ok(())
    }
}

/// Helper function to check minimum argument count
fn check_min_arg_count(args: &[Value], minimum: usize, function_name: &str) -> ExprResult<()> {
    if args.len() < minimum {
        Err(ExprError::InvalidArgCount {
            function: function_name.to_string(),
            expected: minimum,
            found: args.len(),
        })
    } else {
        Ok(())
    }
}

/// Helper to check argument count within a range
fn check_arg_range(
    args: &[Value],
    min: usize,
    max: usize,
    function_name: &str,
) -> ExprResult<()> {
    if args.len() < min || args.len() > max {
        Err(ExprError::InvalidArgCount {
            function: function_name.to_string(),
            expected: min,
            found: args.len(),
        })
    } else {
        Ok(())
    }
}

// ============================================================================
// Mathematical Functions
// ============================================================================

fn fn_abs(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "abs")?;

    match &args[0] {
        Value::Integer(n) => Ok(Value::Integer(n.abs())),
        Value::Decimal(d) => Ok(Value::Decimal(d.abs())),
        Value::Rational(r) => Ok(Value::Rational(r.abs())),
        Value::Amount(a) => Ok(Value::Amount(a.abs())),
        _ => Err(ExprError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: args[0].type_name().to_string(),
            operation: "abs".to_string(),
        }),
    }
}

fn fn_floor(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "floor")?;

    match &args[0] {
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Decimal(d) => Ok(Value::Integer(d.floor().to_i64().unwrap_or(0))),
        Value::Rational(r) => Ok(Value::Integer(r.floor().to_integer().to_i64().unwrap_or(0))),
        _ => Err(ExprError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: args[0].type_name().to_string(),
            operation: "floor".to_string(),
        }),
    }
}

fn fn_ceiling(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "ceiling")?;

    match &args[0] {
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Decimal(d) => Ok(Value::Integer(d.ceil().to_i64().unwrap_or(0))),
        Value::Rational(r) => Ok(Value::Integer(r.ceil().to_integer().to_i64().unwrap_or(0))),
        _ => Err(ExprError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: args[0].type_name().to_string(),
            operation: "ceiling".to_string(),
        }),
    }
}

fn fn_round(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "round")?;

    match &args[0] {
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Decimal(d) => Ok(Value::Integer(d.round().to_i64().unwrap_or(0))),
        Value::Rational(r) => Ok(Value::Integer(r.round().to_integer().to_i64().unwrap_or(0))),
        _ => Err(ExprError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: args[0].type_name().to_string(),
            operation: "round".to_string(),
        }),
    }
}

fn fn_truncate(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "truncate")?;

    match &args[0] {
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Decimal(d) => Ok(Value::Integer(d.trunc().to_i64().unwrap_or(0))),
        Value::Rational(r) => Ok(Value::Integer(r.trunc().to_integer().to_i64().unwrap_or(0))),
        _ => Err(ExprError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: args[0].type_name().to_string(),
            operation: "truncate".to_string(),
        }),
    }
}

fn fn_min(args: &[Value]) -> ExprResult<Value> {
    check_min_arg_count(args, 1, "min")?;

    let mut result = args[0].clone();

    for arg in &args[1..] {
        match (&result, arg) {
            (Value::Integer(a), Value::Integer(b)) => {
                if b < a {
                    result = Value::Integer(*b);
                }
            }
            (Value::Decimal(a), Value::Decimal(b)) => {
                if b < a {
                    result = Value::Decimal(*b);
                }
            }
            (Value::Rational(a), Value::Rational(b)) => {
                if b < a {
                    result = Value::Rational(b.clone());
                }
            }
            _ => {
                return Err(ExprError::TypeMismatch {
                    expected: "comparable numeric types".to_string(),
                    found: format!("{} and {}", result.type_name(), arg.type_name()),
                    operation: "min".to_string(),
                })
            }
        }
    }

    Ok(result)
}

fn fn_max(args: &[Value]) -> ExprResult<Value> {
    check_min_arg_count(args, 1, "max")?;

    let mut result = args[0].clone();

    for arg in &args[1..] {
        match (&result, arg) {
            (Value::Integer(a), Value::Integer(b)) => {
                if b > a {
                    result = Value::Integer(*b);
                }
            }
            (Value::Decimal(a), Value::Decimal(b)) => {
                if b > a {
                    result = Value::Decimal(*b);
                }
            }
            (Value::Rational(a), Value::Rational(b)) => {
                if b > a {
                    result = Value::Rational(b.clone());
                }
            }
            _ => {
                return Err(ExprError::TypeMismatch {
                    expected: "comparable numeric types".to_string(),
                    found: format!("{} and {}", result.type_name(), arg.type_name()),
                    operation: "max".to_string(),
                })
            }
        }
    }

    Ok(result)
}

// ============================================================================
// Date/Time Functions
// ============================================================================

fn fn_now(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 0, "now")?;
    Ok(Value::DateTime(Local::now()))
}

fn fn_today(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 0, "today")?;
    Ok(Value::Date(Local::now().naive_local().date()))
}

fn fn_age(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "age")?;

    match &args[0] {
        Value::Date(date) => {
            let today = Local::now().naive_local().date();
            let diff = today - *date;
            Ok(Value::Integer(diff.num_days()))
        }
        Value::DateTime(datetime) => {
            let now = Local::now();
            let diff = now - *datetime;
            Ok(Value::Integer(diff.num_days()))
        }
        _ => Err(ExprError::TypeMismatch {
            expected: "date or datetime".to_string(),
            found: args[0].type_name().to_string(),
            operation: "age".to_string(),
        }),
    }
}

fn fn_format_date(args: &[Value]) -> ExprResult<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExprError::InvalidArgCount {
            function: "format_date".to_string(),
            expected: 2,
            found: args.len(),
        });
    }

    let format_str = if args.len() == 2 {
        match &args[1] {
            Value::String(s) => s.clone(),
            _ => {
                return Err(ExprError::TypeMismatch {
                    expected: "string".to_string(),
                    found: args[1].type_name().to_string(),
                    operation: "format_date".to_string(),
                })
            }
        }
    } else {
        "%Y-%m-%d".to_string()
    };

    match &args[0] {
        Value::Date(date) => Ok(Value::String(date.format(&format_str).to_string())),
        Value::DateTime(datetime) => Ok(Value::String(datetime.format(&format_str).to_string())),
        _ => Err(ExprError::TypeMismatch {
            expected: "date or datetime".to_string(),
            found: args[0].type_name().to_string(),
            operation: "format_date".to_string(),
        }),
    }
}

// ============================================================================
// String Functions
// ============================================================================

fn fn_format_string(args: &[Value]) -> ExprResult<Value> {
    check_min_arg_count(args, 1, "format")?;

    match &args[0] {
        Value::String(format_str) => {
            let mut result = format_str.clone();
            for (i, arg) in args[1..].iter().enumerate() {
                let placeholder = format!("{{{}}}", i);
                result = result.replace(&placeholder, &arg.to_string());
            }
            Ok(Value::String(result))
        }
        _ => Err(ExprError::TypeMismatch {
            expected: "string".to_string(),
            found: args[0].type_name().to_string(),
            operation: "format".to_string(),
        }),
    }
}

fn fn_to_upper(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_upper")?;

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_uppercase())),
        _ => Err(ExprError::TypeMismatch {
            expected: "string".to_string(),
            found: args[0].type_name().to_string(),
            operation: "to_upper".to_string(),
        }),
    }
}

fn fn_to_lower(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_lower")?;

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_lowercase())),
        _ => Err(ExprError::TypeMismatch {
            expected: "string".to_string(),
            found: args[0].type_name().to_string(),
            operation: "to_lower".to_string(),
        }),
    }
}

fn fn_trim(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "trim")?;

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.trim().to_string())),
        _ => Err(ExprError::TypeMismatch {
            expected: "string".to_string(),
            found: args[0].type_name().to_string(),
            operation: "trim".to_string(),
        }),
    }
}

// ============================================================================
// Type Conversion Functions
// ============================================================================

fn fn_to_string(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_string")?;
    Ok(Value::String(args[0].to_string()))
}

fn fn_to_int(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_int")?;

    match &args[0] {
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Decimal(d) => Ok(Value::Integer(d.to_i64().unwrap_or(0))),
        Value::Rational(r) => Ok(Value::Integer(r.to_integer().to_i64().unwrap_or(0))),
        Value::String(s) => s
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|_| ExprError::RuntimeError(format!("Cannot convert '{}' to integer", s))),
        _ => Err(ExprError::TypeMismatch {
            expected: "convertible to integer".to_string(),
            found: args[0].type_name().to_string(),
            operation: "to_int".to_string(),
        }),
    }
}

fn fn_to_decimal(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_decimal")?;

    match &args[0] {
        Value::Integer(n) => Ok(Value::Decimal(Decimal::from(*n))),
        Value::Decimal(d) => Ok(Value::Decimal(*d)),
        Value::String(s) => s
            .parse::<Decimal>()
            .map(Value::Decimal)
            .map_err(|_| ExprError::RuntimeError(format!("Cannot convert '{}' to decimal", s))),
        _ => Err(ExprError::TypeMismatch {
            expected: "convertible to decimal".to_string(),
            found: args[0].type_name().to_string(),
            operation: "to_decimal".to_string(),
        }),
    }
}

fn fn_to_amount(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_amount")?;

    match &args[0] {
        Value::Integer(n) => Ok(Value::Amount(Amount::from(*n))),
        Value::Decimal(d) => Amount::from_decimal(*d)
            .map(Value::Amount)
            .map_err(|e| ExprError::RuntimeError(e.to_string())),
        Value::Rational(r) => Ok(Value::Amount(Amount::from_rational(r.clone()))),
        Value::Amount(a) => Ok(Value::Amount(a.clone())),
        Value::String(s) => s
            .parse::<Amount>()
            .map(Value::Amount)
            .map_err(|_| ExprError::RuntimeError(format!("Cannot convert '{}' to amount", s))),
        _ => Err(ExprError::TypeMismatch {
            expected: "convertible to amount".to_string(),
            found: args[0].type_name().to_string(),
            operation: "to_amount".to_string(),
        }),
    }
}

fn fn_to_boolean(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_boolean")?;
    Ok(Value::Bool(args[0].is_truthy()))
}

fn fn_to_date(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_date")?;

    match &args[0] {
        Value::Date(d) => Ok(Value::Date(*d)),
        Value::DateTime(dt) => Ok(Value::Date(dt.naive_local().date())),
        Value::String(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .or_else(|_| NaiveDate::parse_from_str(s, "%Y/%m/%d"))
            .map(Value::Date)
            .map_err(|_| ExprError::RuntimeError(format!("Cannot parse '{}' as date", s))),
        _ => Err(ExprError::TypeMismatch {
            expected: "convertible to date".to_string(),
            found: args[0].type_name().to_string(),
            operation: "to_date".to_string(),
        }),
    }
}

fn fn_to_datetime(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_datetime")?;

    match &args[0] {
        Value::DateTime(dt) => Ok(Value::DateTime(*dt)),
        Value::Date(d) => {
            let dt = d
                .and_hms_opt(0, 0, 0)
                .and_then(|ndt| Local.from_local_datetime(&ndt).single());
            match dt {
                Some(dt) => Ok(Value::DateTime(dt)),
                None => Err(ExprError::RuntimeError(format!(
                    "Cannot convert date {} to datetime",
                    d
                ))),
            }
        }
        _ => Err(ExprError::TypeMismatch {
            expected: "convertible to datetime".to_string(),
            found: args[0].type_name().to_string(),
            operation: "to_datetime".to_string(),
        }),
    }
}

fn fn_to_balance(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "to_balance")?;

    match &args[0] {
        Value::Amount(a) => {
            // Validate via Balance::from_amount, but return as Amount since
            // the expression engine's Value type doesn't have a Balance variant.
            let _bal = Balance::from_amount(a.clone())
                .map_err(|e| ExprError::RuntimeError(e.to_string()))?;
            Ok(Value::Amount(a.clone()))
        }
        Value::Integer(n) => Ok(Value::Amount(Amount::from(*n))),
        Value::Decimal(d) => Amount::from_decimal(*d)
            .map(Value::Amount)
            .map_err(|e| ExprError::RuntimeError(e.to_string())),
        _ => Err(ExprError::TypeMismatch {
            expected: "amount or numeric".to_string(),
            found: args[0].type_name().to_string(),
            operation: "to_balance".to_string(),
        }),
    }
}

// ============================================================================
// Aggregation Functions
// ============================================================================

fn fn_sum(args: &[Value]) -> ExprResult<Value> {
    if args.is_empty() {
        return Ok(Value::Integer(0));
    }

    match &args[0] {
        Value::Sequence(seq) => {
            let mut result = Value::Integer(0);
            for item in seq {
                result = add_values(&result, item)?;
            }
            Ok(result)
        }
        _ => {
            let mut result = args[0].clone();
            for arg in &args[1..] {
                result = add_values(&result, arg)?;
            }
            Ok(result)
        }
    }
}

fn fn_count(args: &[Value]) -> ExprResult<Value> {
    if args.is_empty() {
        return Ok(Value::Integer(0));
    }

    match &args[0] {
        Value::Sequence(seq) => Ok(Value::Integer(seq.len() as i64)),
        _ => Ok(Value::Integer(args.len() as i64)),
    }
}

fn fn_average(args: &[Value]) -> ExprResult<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let sum = fn_sum(args)?;
    let count = fn_count(args)?;

    match (sum, count) {
        (sum_val, Value::Integer(n)) if n > 0 => match sum_val {
            Value::Integer(s) => Ok(Value::Rational(BigRational::new(s.into(), n.into()))),
            Value::Decimal(s) => Ok(Value::Decimal(s / Decimal::from(n))),
            Value::Rational(s) => Ok(Value::Rational(s / BigRational::from(BigInt::from(n)))),
            _ => Ok(Value::Null),
        },
        _ => Ok(Value::Null),
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

fn fn_is_empty(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "is_empty")?;

    let is_empty = match &args[0] {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Sequence(seq) => seq.is_empty(),
        Value::Integer(n) => *n == 0,
        Value::Decimal(d) => d.is_zero(),
        Value::Rational(r) => r.is_zero(),
        Value::Amount(a) => a.is_zero(),
        _ => false,
    };

    Ok(Value::Bool(is_empty))
}

fn fn_length(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "length")?;

    match &args[0] {
        Value::String(s) => Ok(Value::Integer(s.len() as i64)),
        Value::Sequence(seq) => Ok(Value::Integer(seq.len() as i64)),
        _ => Err(ExprError::TypeMismatch {
            expected: "string or sequence".to_string(),
            found: args[0].type_name().to_string(),
            operation: "length".to_string(),
        }),
    }
}

fn fn_type(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "type")?;
    Ok(Value::String(args[0].type_name().to_string()))
}

// ============================================================================
// Amount/Value Functions
// ============================================================================

/// `quantity(amount)` — return the numeric part without commodity
fn fn_quantity(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "quantity")?;

    match &args[0] {
        Value::Amount(a) => Ok(Value::Decimal(a.value())),
        Value::Integer(n) => Ok(Value::Decimal(Decimal::from(*n))),
        Value::Decimal(d) => Ok(Value::Decimal(*d)),
        Value::Null => Ok(Value::Decimal(Decimal::ZERO)),
        _ => Err(ExprError::TypeMismatch {
            expected: "amount or numeric".to_string(),
            found: args[0].type_name().to_string(),
            operation: "quantity".to_string(),
        }),
    }
}

/// `commodity(amount)` — return commodity symbol as string
fn fn_commodity(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "commodity")?;

    match &args[0] {
        Value::Amount(a) => {
            let symbol = a
                .commodity()
                .map(|c| c.symbol().to_string())
                .unwrap_or_default();
            Ok(Value::String(symbol))
        }
        Value::Null => Ok(Value::String(String::new())),
        _ => Err(ExprError::TypeMismatch {
            expected: "amount".to_string(),
            found: args[0].type_name().to_string(),
            operation: "commodity".to_string(),
        }),
    }
}

/// `rounded(amount)` — round to commodity display precision
fn fn_rounded(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "rounded")?;

    match &args[0] {
        Value::Amount(a) => Ok(Value::Amount(a.rounded())),
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Decimal(d) => Ok(Value::Decimal(d.round())),
        Value::Null => Ok(Value::Null),
        _ => Err(ExprError::TypeMismatch {
            expected: "amount or numeric".to_string(),
            found: args[0].type_name().to_string(),
            operation: "rounded".to_string(),
        }),
    }
}

/// `unrounded(amount)` — return with full internal precision
fn fn_unrounded(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "unrounded")?;

    match &args[0] {
        Value::Amount(a) => Ok(Value::Amount(a.unrounded())),
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Decimal(d) => Ok(Value::Decimal(*d)),
        Value::Null => Ok(Value::Null),
        _ => Err(ExprError::TypeMismatch {
            expected: "amount or numeric".to_string(),
            found: args[0].type_name().to_string(),
            operation: "unrounded".to_string(),
        }),
    }
}

/// `truncated(amount)` — truncate to commodity display precision
fn fn_truncated(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "truncated")?;

    match &args[0] {
        Value::Amount(a) => Ok(Value::Amount(a.truncated())),
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Decimal(d) => Ok(Value::Decimal(d.trunc())),
        Value::Null => Ok(Value::Null),
        _ => Err(ExprError::TypeMismatch {
            expected: "amount or numeric".to_string(),
            found: args[0].type_name().to_string(),
            operation: "truncated".to_string(),
        }),
    }
}

/// `strip(amount)` — strip annotations (lot prices, dates, tags)
fn fn_strip(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "strip")?;

    match &args[0] {
        Value::Amount(a) => Ok(Value::Amount(a.strip_annotations(&KeepDetails::none()))),
        Value::Null => Ok(Value::Null),
        other => Ok(other.clone()),
    }
}

/// `scrub(amount)` — strip annotations + round (most common in format strings)
fn fn_scrub(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "scrub")?;

    match &args[0] {
        Value::Amount(a) => {
            let stripped = a.strip_annotations(&KeepDetails::none());
            Ok(Value::Amount(stripped.rounded()))
        }
        Value::Null => Ok(Value::Null),
        other => Ok(other.clone()),
    }
}

/// `market(amount)` — return market value (stub: returns amount as-is)
fn fn_market(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "market")?;
    Ok(args[0].clone())
}

/// `exchange(amount, commodity)` — convert to commodity (stub: returns amount as-is)
fn fn_exchange(args: &[Value]) -> ExprResult<Value> {
    check_arg_range(args, 1, 2, "exchange")?;
    Ok(args[0].clone())
}

// ============================================================================
// Display Functions
// ============================================================================

/// `display_amount(amount)` — format for display (rounded with commodity)
fn fn_display_amount(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "display_amount")?;

    match &args[0] {
        Value::Amount(a) => {
            let rounded = a.rounded();
            Ok(Value::String(format!("{}", rounded)))
        }
        Value::Null => Ok(Value::String("0".to_string())),
        other => Ok(Value::String(format!("{}", other))),
    }
}

/// `display_total(total)` — format total for display
fn fn_display_total(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "display_total")?;

    match &args[0] {
        Value::Amount(a) => {
            let rounded = a.rounded();
            Ok(Value::String(format!("{}", rounded)))
        }
        Value::Null => Ok(Value::String("0".to_string())),
        other => Ok(Value::String(format!("{}", other))),
    }
}

/// `justify(str, width, first_col, separate)` — right-justify string to width
fn fn_justify(args: &[Value]) -> ExprResult<Value> {
    check_arg_range(args, 2, 4, "justify")?;

    let text = match &args[0] {
        Value::String(s) => s.clone(),
        other => format!("{}", other),
    };

    let width = match &args[1] {
        Value::Integer(n) => *n as usize,
        _ => {
            return Err(ExprError::TypeMismatch {
                expected: "integer".to_string(),
                found: args[1].type_name().to_string(),
                operation: "justify".to_string(),
            })
        }
    };

    let first_col = if args.len() > 2 { args[2].is_truthy() } else { false };

    if first_col || text.len() >= width {
        Ok(Value::String(text))
    } else {
        Ok(Value::String(format!("{:>width$}", text, width = width)))
    }
}

/// `quoted(str)` — wrap in double quotes
fn fn_quoted(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "quoted")?;

    let text = match &args[0] {
        Value::String(s) => s.clone(),
        other => format!("{}", other),
    };

    Ok(Value::String(format!("\"{}\"", text)))
}

/// `quoted_rfc(str)` — RFC-compliant quoting with escapes
fn fn_quoted_rfc(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "quoted_rfc")?;

    let text = match &args[0] {
        Value::String(s) => s.clone(),
        other => format!("{}", other),
    };

    // RFC 4180 CSV-style quoting: double any internal quotes
    let escaped = text.replace('"', "\"\"");
    Ok(Value::String(format!("\"{}\"", escaped)))
}

/// `ansify_if(str, color)` — apply ANSI color (stub: return str)
fn fn_ansify_if(args: &[Value]) -> ExprResult<Value> {
    check_arg_range(args, 1, 2, "ansify_if")?;
    Ok(args[0].clone())
}

/// `should_bold()` — whether to use bold (stub: return false)
fn fn_should_bold(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 0, "should_bold")?;
    Ok(Value::Bool(false))
}

// ============================================================================
// Additional Utility Functions
// ============================================================================

/// `percent(amount, total)` — calculate percentage
fn fn_percent(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 2, "percent")?;

    let hundred = Decimal::from(100);

    match (&args[0], &args[1]) {
        (Value::Amount(a), Value::Amount(b)) => {
            if b.is_zero() {
                return Ok(Value::Decimal(Decimal::ZERO));
            }
            let a_val = a.value();
            let b_val = b.value();
            if b_val.is_zero() {
                Ok(Value::Decimal(Decimal::ZERO))
            } else {
                Ok(Value::Decimal(a_val / b_val * hundred))
            }
        }
        (Value::Decimal(a), Value::Decimal(b)) => {
            if b.is_zero() {
                Ok(Value::Decimal(Decimal::ZERO))
            } else {
                Ok(Value::Decimal(*a / *b * hundred))
            }
        }
        (Value::Integer(a), Value::Integer(b)) => {
            if *b == 0 {
                Ok(Value::Decimal(Decimal::ZERO))
            } else {
                Ok(Value::Decimal(Decimal::from(*a) / Decimal::from(*b) * hundred))
            }
        }
        _ => Err(ExprError::TypeMismatch {
            expected: "numeric types".to_string(),
            found: format!("{} and {}", args[0].type_name(), args[1].type_name()),
            operation: "percent".to_string(),
        }),
    }
}

/// `join(sequence)` — join sequence elements to string
fn fn_join(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "join")?;

    match &args[0] {
        Value::Sequence(seq) => {
            let parts: Vec<String> = seq.iter().map(|v| format!("{}", v)).collect();
            Ok(Value::String(parts.join("\n")))
        }
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Null => Ok(Value::String(String::new())),
        _ => Err(ExprError::TypeMismatch {
            expected: "sequence".to_string(),
            found: args[0].type_name().to_string(),
            operation: "join".to_string(),
        }),
    }
}

/// `get_at(sequence, index)` — get element at index
fn fn_get_at(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 2, "get_at")?;

    let idx = match &args[1] {
        Value::Integer(n) => *n,
        _ => {
            return Err(ExprError::TypeMismatch {
                expected: "integer".to_string(),
                found: args[1].type_name().to_string(),
                operation: "get_at".to_string(),
            })
        }
    };

    match &args[0] {
        Value::Sequence(seq) => {
            if idx < 0 || idx as usize >= seq.len() {
                Ok(Value::Null)
            } else {
                Ok(seq[idx as usize].clone())
            }
        }
        Value::String(s) => {
            if idx < 0 || idx as usize >= s.len() {
                Ok(Value::Null)
            } else {
                Ok(Value::String(
                    s.chars()
                        .nth(idx as usize)
                        .map(|c| c.to_string())
                        .unwrap_or_default(),
                ))
            }
        }
        _ => Err(ExprError::TypeMismatch {
            expected: "sequence or string".to_string(),
            found: args[0].type_name().to_string(),
            operation: "get_at".to_string(),
        }),
    }
}

/// `is_seq(value)` — check if value is a sequence
fn fn_is_seq(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "is_seq")?;
    Ok(Value::Bool(matches!(&args[0], Value::Sequence(_))))
}

// ============================================================================
// Lot/Annotation Functions
// ============================================================================

/// `lot_date(amount)` — return lot date from annotation
fn fn_lot_date(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "lot_date")?;

    match &args[0] {
        Value::Amount(a) => {
            if let Some(commodity) = a.commodity() {
                if commodity.has_annotation() {
                    if let Some(date) = commodity.annotation().date() {
                        return Ok(Value::Date(*date));
                    }
                }
            }
            Ok(Value::Null)
        }
        Value::Null => Ok(Value::Null),
        _ => Err(ExprError::TypeMismatch {
            expected: "amount".to_string(),
            found: args[0].type_name().to_string(),
            operation: "lot_date".to_string(),
        }),
    }
}

/// `lot_price(amount)` — return lot price from annotation
fn fn_lot_price(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "lot_price")?;

    match &args[0] {
        Value::Amount(a) => {
            if let Some(commodity) = a.commodity() {
                if commodity.has_annotation() {
                    if let Some(price) = commodity.annotation().price() {
                        return Ok(Value::Amount(price.clone()));
                    }
                }
            }
            Ok(Value::Null)
        }
        Value::Null => Ok(Value::Null),
        _ => Err(ExprError::TypeMismatch {
            expected: "amount".to_string(),
            found: args[0].type_name().to_string(),
            operation: "lot_price".to_string(),
        }),
    }
}

/// `lot_tag(amount)` — return lot tag from annotation
fn fn_lot_tag(args: &[Value]) -> ExprResult<Value> {
    check_arg_count(args, 1, "lot_tag")?;

    match &args[0] {
        Value::Amount(a) => {
            if let Some(commodity) = a.commodity() {
                if commodity.has_annotation() {
                    if let Some(tag) = commodity.annotation().tag() {
                        return Ok(Value::String(tag.clone()));
                    }
                }
            }
            Ok(Value::Null)
        }
        Value::Null => Ok(Value::Null),
        _ => Err(ExprError::TypeMismatch {
            expected: "amount".to_string(),
            found: args[0].type_name().to_string(),
            operation: "lot_tag".to_string(),
        }),
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn add_values(left: &Value, right: &Value) -> ExprResult<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
        (Value::Decimal(a), Value::Decimal(b)) => Ok(Value::Decimal(a + b)),
        (Value::Rational(a), Value::Rational(b)) => Ok(Value::Rational(a + b)),
        (Value::Integer(a), Value::Decimal(b)) => Ok(Value::Decimal(Decimal::from(*a) + b)),
        (Value::Decimal(a), Value::Integer(b)) => Ok(Value::Decimal(a + Decimal::from(*b))),
        _ => Err(ExprError::RuntimeError(format!(
            "Cannot add {} and {}",
            left.type_name(),
            right.type_name()
        ))),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_math::{Annotation, Commodity, CommodityRef};
    use std::sync::Arc;

    // --- Existing tests (preserved) ---

    #[test]
    fn test_abs_function() {
        let args = vec![Value::Integer(-42)];
        let result = fn_abs(&args).unwrap();
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_max_function() {
        let args = vec![Value::Integer(1), Value::Integer(5), Value::Integer(3)];
        let result = fn_max(&args).unwrap();
        assert_eq!(result, Value::Integer(5));
    }

    #[test]
    fn test_min_function() {
        let args = vec![Value::Integer(1), Value::Integer(5), Value::Integer(3)];
        let result = fn_min(&args).unwrap();
        assert_eq!(result, Value::Integer(1));
    }

    #[test]
    fn test_to_upper_function() {
        let args = vec![Value::String("hello".to_string())];
        let result = fn_to_upper(&args).unwrap();
        assert_eq!(result, Value::String("HELLO".to_string()));
    }

    #[test]
    fn test_length_function() {
        let args = vec![Value::String("hello".to_string())];
        let result = fn_length(&args).unwrap();
        assert_eq!(result, Value::Integer(5));
    }

    #[test]
    fn test_sum_function() {
        let args = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        let result = fn_sum(&args).unwrap();
        assert_eq!(result, Value::Integer(6));
    }

    #[test]
    fn test_count_function() {
        let args = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        let result = fn_count(&args).unwrap();
        assert_eq!(result, Value::Integer(3));
    }

    #[test]
    fn test_type_function() {
        let args = vec![Value::Integer(42)];
        let result = fn_type(&args).unwrap();
        assert_eq!(result, Value::String("integer".to_string()));
    }

    #[test]
    fn test_is_empty_function() {
        let args = vec![Value::String("".to_string())];
        let result = fn_is_empty(&args).unwrap();
        assert_eq!(result, Value::Bool(true));

        let args = vec![Value::String("hello".to_string())];
        let result = fn_is_empty(&args).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    // --- New function tests ---

    fn make_amount(val: i64) -> Amount {
        Amount::from(val)
    }

    fn make_amount_with_commodity(val: i64, sym: &str) -> Amount {
        let commodity: CommodityRef = Arc::new(Commodity::new(sym));
        Amount::with_commodity(Decimal::from(val), Some(commodity))
    }

    // quantity
    #[test]
    fn test_quantity_amount() {
        let a = make_amount_with_commodity(42, "$");
        let args = vec![Value::Amount(a)];
        let result = fn_quantity(&args).unwrap();
        assert_eq!(result, Value::Decimal(Decimal::from(42)));
    }

    #[test]
    fn test_quantity_integer() {
        let args = vec![Value::Integer(10)];
        let result = fn_quantity(&args).unwrap();
        assert_eq!(result, Value::Decimal(Decimal::from(10)));
    }

    #[test]
    fn test_quantity_null() {
        let args = vec![Value::Null];
        let result = fn_quantity(&args).unwrap();
        assert_eq!(result, Value::Decimal(Decimal::ZERO));
    }

    #[test]
    fn test_quantity_type_error() {
        let args = vec![Value::String("oops".to_string())];
        assert!(fn_quantity(&args).is_err());
    }

    // commodity
    #[test]
    fn test_commodity_with_symbol() {
        let a = make_amount_with_commodity(100, "USD");
        let args = vec![Value::Amount(a)];
        let result = fn_commodity(&args).unwrap();
        assert_eq!(result, Value::String("USD".to_string()));
    }

    #[test]
    fn test_commodity_no_symbol() {
        let a = make_amount(100);
        let args = vec![Value::Amount(a)];
        let result = fn_commodity(&args).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_commodity_null() {
        let args = vec![Value::Null];
        let result = fn_commodity(&args).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    // rounded
    #[test]
    fn test_rounded_amount() {
        let a = make_amount(42);
        let args = vec![Value::Amount(a)];
        let result = fn_rounded(&args).unwrap();
        assert!(matches!(result, Value::Amount(_)));
    }

    #[test]
    fn test_rounded_null() {
        let args = vec![Value::Null];
        let result = fn_rounded(&args).unwrap();
        assert_eq!(result, Value::Null);
    }

    // unrounded
    #[test]
    fn test_unrounded_amount() {
        let a = make_amount(42);
        let args = vec![Value::Amount(a)];
        let result = fn_unrounded(&args).unwrap();
        assert!(matches!(result, Value::Amount(_)));
    }

    // truncated
    #[test]
    fn test_truncated_amount() {
        let a = make_amount(42);
        let args = vec![Value::Amount(a)];
        let result = fn_truncated(&args).unwrap();
        assert!(matches!(result, Value::Amount(_)));
    }

    // strip
    #[test]
    fn test_strip_amount() {
        let a = make_amount_with_commodity(50, "EUR");
        let args = vec![Value::Amount(a)];
        let result = fn_strip(&args).unwrap();
        assert!(matches!(result, Value::Amount(_)));
    }

    #[test]
    fn test_strip_non_amount() {
        let args = vec![Value::Integer(42)];
        let result = fn_strip(&args).unwrap();
        assert_eq!(result, Value::Integer(42));
    }

    // scrub
    #[test]
    fn test_scrub_amount() {
        let a = make_amount_with_commodity(50, "EUR");
        let args = vec![Value::Amount(a)];
        let result = fn_scrub(&args).unwrap();
        assert!(matches!(result, Value::Amount(_)));
    }

    // market (stub)
    #[test]
    fn test_market_passthrough() {
        let a = make_amount(100);
        let args = vec![Value::Amount(a.clone())];
        let result = fn_market(&args).unwrap();
        assert_eq!(result, Value::Amount(a));
    }

    // exchange (stub)
    #[test]
    fn test_exchange_passthrough() {
        let a = make_amount(100);
        let args = vec![Value::Amount(a.clone()), Value::String("USD".to_string())];
        let result = fn_exchange(&args).unwrap();
        assert_eq!(result, Value::Amount(a));
    }

    #[test]
    fn test_exchange_single_arg() {
        let a = make_amount(100);
        let args = vec![Value::Amount(a.clone())];
        let result = fn_exchange(&args).unwrap();
        assert_eq!(result, Value::Amount(a));
    }

    // display_amount
    #[test]
    fn test_display_amount() {
        let a = make_amount(42);
        let args = vec![Value::Amount(a)];
        let result = fn_display_amount(&args).unwrap();
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_display_amount_null() {
        let args = vec![Value::Null];
        let result = fn_display_amount(&args).unwrap();
        assert_eq!(result, Value::String("0".to_string()));
    }

    // display_total
    #[test]
    fn test_display_total() {
        let a = make_amount(42);
        let args = vec![Value::Amount(a)];
        let result = fn_display_total(&args).unwrap();
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_display_total_null() {
        let args = vec![Value::Null];
        let result = fn_display_total(&args).unwrap();
        assert_eq!(result, Value::String("0".to_string()));
    }

    // justify
    #[test]
    fn test_justify_right() {
        let args = vec![Value::String("hi".to_string()), Value::Integer(10)];
        let result = fn_justify(&args).unwrap();
        if let Value::String(s) = result {
            assert_eq!(s.len(), 10);
            assert!(s.ends_with("hi"));
        } else {
            panic!("expected string");
        }
    }

    #[test]
    fn test_justify_first_col() {
        let args = vec![
            Value::String("hi".to_string()),
            Value::Integer(10),
            Value::Bool(true),
        ];
        let result = fn_justify(&args).unwrap();
        assert_eq!(result, Value::String("hi".to_string()));
    }

    // quoted
    #[test]
    fn test_quoted() {
        let args = vec![Value::String("hello".to_string())];
        let result = fn_quoted(&args).unwrap();
        assert_eq!(result, Value::String("\"hello\"".to_string()));
    }

    // quoted_rfc
    #[test]
    fn test_quoted_rfc_escapes() {
        let args = vec![Value::String("say \"hi\"".to_string())];
        let result = fn_quoted_rfc(&args).unwrap();
        assert_eq!(result, Value::String("\"say \"\"hi\"\"\"".to_string()));
    }

    // ansify_if (stub)
    #[test]
    fn test_ansify_if_passthrough() {
        let args = vec![
            Value::String("text".to_string()),
            Value::String("red".to_string()),
        ];
        let result = fn_ansify_if(&args).unwrap();
        assert_eq!(result, Value::String("text".to_string()));
    }

    // should_bold (stub)
    #[test]
    fn test_should_bold() {
        let args: Vec<Value> = vec![];
        let result = fn_should_bold(&args).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    // to_boolean
    #[test]
    fn test_to_boolean_truthy() {
        assert_eq!(
            fn_to_boolean(&[Value::Integer(1)]).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            fn_to_boolean(&[Value::Integer(0)]).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(fn_to_boolean(&[Value::Null]).unwrap(), Value::Bool(false));
        assert_eq!(
            fn_to_boolean(&[Value::String("hi".to_string())]).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            fn_to_boolean(&[Value::String("".to_string())]).unwrap(),
            Value::Bool(false)
        );
    }

    // to_date
    #[test]
    fn test_to_date_from_string() {
        let args = vec![Value::String("2024-01-15".to_string())];
        let result = fn_to_date(&args).unwrap();
        assert_eq!(
            result,
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        );
    }

    #[test]
    fn test_to_date_slash_format() {
        let args = vec![Value::String("2024/01/15".to_string())];
        let result = fn_to_date(&args).unwrap();
        assert_eq!(
            result,
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        );
    }

    #[test]
    fn test_to_date_invalid() {
        let args = vec![Value::String("not-a-date".to_string())];
        assert!(fn_to_date(&args).is_err());
    }

    // to_datetime
    #[test]
    fn test_to_datetime_from_date() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let args = vec![Value::Date(date)];
        let result = fn_to_datetime(&args).unwrap();
        assert!(matches!(result, Value::DateTime(_)));
    }

    // to_balance
    #[test]
    fn test_to_balance_amount() {
        let a = make_amount(42);
        let args = vec![Value::Amount(a.clone())];
        let result = fn_to_balance(&args).unwrap();
        assert_eq!(result, Value::Amount(a));
    }

    #[test]
    fn test_to_balance_integer() {
        let args = vec![Value::Integer(42)];
        let result = fn_to_balance(&args).unwrap();
        assert!(matches!(result, Value::Amount(_)));
    }

    // percent
    #[test]
    fn test_percent_integers() {
        let args = vec![Value::Integer(25), Value::Integer(100)];
        let result = fn_percent(&args).unwrap();
        assert_eq!(result, Value::Decimal(Decimal::from(25)));
    }

    #[test]
    fn test_percent_zero_total() {
        let args = vec![Value::Integer(25), Value::Integer(0)];
        let result = fn_percent(&args).unwrap();
        assert_eq!(result, Value::Decimal(Decimal::ZERO));
    }

    #[test]
    fn test_percent_amounts() {
        let a = make_amount(50);
        let b = make_amount(200);
        let args = vec![Value::Amount(a), Value::Amount(b)];
        let result = fn_percent(&args).unwrap();
        assert_eq!(result, Value::Decimal(Decimal::from(25)));
    }

    // join
    #[test]
    fn test_join_sequence() {
        let args = vec![Value::Sequence(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ])];
        let result = fn_join(&args).unwrap();
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_join_null() {
        let args = vec![Value::Null];
        let result = fn_join(&args).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    // get_at
    #[test]
    fn test_get_at_sequence() {
        let args = vec![
            Value::Sequence(vec![
                Value::Integer(10),
                Value::Integer(20),
                Value::Integer(30),
            ]),
            Value::Integer(1),
        ];
        let result = fn_get_at(&args).unwrap();
        assert_eq!(result, Value::Integer(20));
    }

    #[test]
    fn test_get_at_out_of_bounds() {
        let args = vec![
            Value::Sequence(vec![Value::Integer(10)]),
            Value::Integer(5),
        ];
        let result = fn_get_at(&args).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_get_at_negative_index() {
        let args = vec![
            Value::Sequence(vec![Value::Integer(10)]),
            Value::Integer(-1),
        ];
        let result = fn_get_at(&args).unwrap();
        assert_eq!(result, Value::Null);
    }

    // is_seq
    #[test]
    fn test_is_seq_true() {
        let args = vec![Value::Sequence(vec![])];
        let result = fn_is_seq(&args).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_is_seq_false() {
        let args = vec![Value::Integer(42)];
        let result = fn_is_seq(&args).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    // lot_date
    #[test]
    fn test_lot_date_no_annotation() {
        let a = make_amount_with_commodity(100, "AAPL");
        let args = vec![Value::Amount(a)];
        let result = fn_lot_date(&args).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_lot_date_with_annotation() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let mut a = make_amount_with_commodity(100, "AAPL");
        a.annotate(Annotation::with_date(date));
        let args = vec![Value::Amount(a)];
        let result = fn_lot_date(&args).unwrap();
        assert_eq!(result, Value::Date(date));
    }

    #[test]
    fn test_lot_date_null() {
        let args = vec![Value::Null];
        let result = fn_lot_date(&args).unwrap();
        assert_eq!(result, Value::Null);
    }

    // lot_price
    #[test]
    fn test_lot_price_no_annotation() {
        let a = make_amount_with_commodity(100, "AAPL");
        let args = vec![Value::Amount(a)];
        let result = fn_lot_price(&args).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_lot_price_with_annotation() {
        let price = make_amount_with_commodity(150, "$");
        let mut a = make_amount_with_commodity(100, "AAPL");
        a.annotate(Annotation::with_price(price.clone()));
        let args = vec![Value::Amount(a)];
        let result = fn_lot_price(&args).unwrap();
        assert_eq!(result, Value::Amount(price));
    }

    // lot_tag
    #[test]
    fn test_lot_tag_no_annotation() {
        let a = make_amount_with_commodity(100, "AAPL");
        let args = vec![Value::Amount(a)];
        let result = fn_lot_tag(&args).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_lot_tag_type_error() {
        let args = vec![Value::String("not an amount".to_string())];
        assert!(fn_lot_tag(&args).is_err());
    }
}
