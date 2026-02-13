//! Scope trait hierarchy for expression evaluation
//!
//! This module provides the core `Scope` trait that enables expressions to be
//! evaluated against structured data (postings, transactions, accounts) via
//! name lookup, mirroring the C++ ledger's `scope_t` hierarchy.

use super::Value;
use std::collections::HashMap;

/// Core trait for expression evaluation scopes.
///
/// Types implementing `Scope` can participate in the expression evaluation
/// chain. When an expression references a variable by name, the scope chain
/// is walked via `lookup()` and `parent()` until the name resolves.
pub trait Scope {
    /// Look up a named value in this scope only (not parent).
    fn lookup(&self, name: &str) -> Option<Value>;

    /// Get parent scope for chain traversal.
    fn parent(&self) -> Option<&dyn Scope>;

    /// Description for error messages.
    fn description(&self) -> &str;
}

/// Walk up the scope chain to resolve a name.
///
/// Checks the given scope first, then walks to its parent, grandparent, etc.
/// Returns `None` if the name is not found in any scope.
pub fn resolve(scope: &dyn Scope, name: &str) -> Option<Value> {
    scope
        .lookup(name)
        .or_else(|| scope.parent().and_then(|p| resolve(p, name)))
}

/// HashMap-backed scope for ad-hoc variable bindings.
///
/// Useful for injecting temporary variables into a scope chain without
/// needing a dedicated struct.
pub struct BindScope<'a> {
    bindings: HashMap<String, Value>,
    parent: Option<&'a dyn Scope>,
}

impl<'a> BindScope<'a> {
    /// Create a new `BindScope` with the given parent.
    pub fn new(parent: &'a dyn Scope) -> Self {
        Self { bindings: HashMap::new(), parent: Some(parent) }
    }

    /// Create a `BindScope` with no parent.
    pub fn empty() -> Self {
        Self { bindings: HashMap::new(), parent: None }
    }

    /// Define a variable in this scope.
    pub fn define(&mut self, name: impl Into<String>, value: Value) {
        self.bindings.insert(name.into(), value);
    }
}

impl Scope for BindScope<'_> {
    fn lookup(&self, name: &str) -> Option<Value> {
        self.bindings.get(name).cloned()
    }

    fn parent(&self) -> Option<&dyn Scope> {
        self.parent.map(|p| p as &dyn Scope)
    }

    fn description(&self) -> &str {
        "bind_scope"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bind_scope_define_and_lookup() {
        let mut scope = BindScope::empty();
        scope.define("x", Value::Integer(42));
        scope.define("name", Value::String("hello".into()));

        assert_eq!(scope.lookup("x"), Some(Value::Integer(42)));
        assert_eq!(scope.lookup("name"), Some(Value::String("hello".into())));
        assert_eq!(scope.lookup("missing"), None);
    }

    #[test]
    fn test_bind_scope_parent_chain() {
        let mut parent = BindScope::empty();
        parent.define("x", Value::Integer(10));
        parent.define("y", Value::Integer(20));

        let mut child = BindScope::new(&parent);
        child.define("x", Value::Integer(99)); // shadows parent
        child.define("z", Value::Integer(30));

        // Direct lookup only checks local scope
        assert_eq!(child.lookup("x"), Some(Value::Integer(99)));
        assert_eq!(child.lookup("z"), Some(Value::Integer(30)));
        assert_eq!(child.lookup("y"), None); // not in child directly
    }

    #[test]
    fn test_resolve_walks_chain() {
        let mut parent = BindScope::empty();
        parent.define("x", Value::Integer(10));
        parent.define("y", Value::Integer(20));

        let mut child = BindScope::new(&parent);
        child.define("x", Value::Integer(99)); // shadows parent
        child.define("z", Value::Integer(30));

        // resolve() walks the chain
        assert_eq!(resolve(&child, "x"), Some(Value::Integer(99))); // child shadows parent
        assert_eq!(resolve(&child, "y"), Some(Value::Integer(20))); // found in parent
        assert_eq!(resolve(&child, "z"), Some(Value::Integer(30))); // found in child
        assert_eq!(resolve(&child, "missing"), None); // not found anywhere
    }

    #[test]
    fn test_resolve_three_level_chain() {
        let mut grandparent = BindScope::empty();
        grandparent.define("a", Value::Integer(1));

        let mut parent = BindScope::new(&grandparent);
        parent.define("b", Value::Integer(2));

        let mut child = BindScope::new(&parent);
        child.define("c", Value::Integer(3));

        assert_eq!(resolve(&child, "a"), Some(Value::Integer(1)));
        assert_eq!(resolve(&child, "b"), Some(Value::Integer(2)));
        assert_eq!(resolve(&child, "c"), Some(Value::Integer(3)));
        assert_eq!(resolve(&child, "d"), None);
    }

    #[test]
    fn test_bind_scope_empty_has_no_parent() {
        let scope = BindScope::empty();
        assert!(scope.parent().is_none());
        assert_eq!(scope.description(), "bind_scope");
    }

    #[test]
    fn test_bind_scope_overwrite() {
        let mut scope = BindScope::empty();
        scope.define("x", Value::Integer(1));
        scope.define("x", Value::Integer(2));

        assert_eq!(scope.lookup("x"), Some(Value::Integer(2)));
    }
}
