use std::any::Any;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;

use vortex_array::compute;
use vortex_array::{Array, ArrayRef};
use vortex_dtype::{DType, Nullability};
use vortex_error::VortexResult;
use vortex_expr::{ExprRef, VortexExpr};
use vortex_scalar::Scalar;

#[derive(Debug, Clone, Eq, Hash)]
#[allow(clippy::derived_hash_with_manual_eq)]
pub struct ListContainsExpr {
    lhs: ExprRef,
    value: Scalar,
}

impl ListContainsExpr {
    pub fn new_expr(lhs: ExprRef, value: Scalar) -> ExprRef {
        Arc::new(Self { lhs, value })
    }
}

impl Display for ListContainsExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} contains {})", self.lhs, self.value)
    }
}

impl VortexExpr for ListContainsExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn unchecked_evaluate(&self, batch: &dyn Array) -> VortexResult<ArrayRef> {
        let lhs = self.lhs.evaluate(batch)?;

        compute::list_contains(&lhs, self.value.clone())
    }

    fn children(&self) -> Vec<&ExprRef> {
        vec![&self.lhs]
    }

    fn replacing_children(self: Arc<Self>, children: Vec<ExprRef>) -> ExprRef {
        assert_eq!(children.len(), 1);
        ListContainsExpr::new_expr(children[0].clone(), self.value.clone())
    }

    fn return_dtype(&self, _scope_dtype: &DType) -> VortexResult<DType> {
        Ok(DType::Bool(Nullability::NonNullable))
    }
}

impl PartialEq for ListContainsExpr {
    fn eq(&self, other: &ListContainsExpr) -> bool {
        other.lhs.eq(&self.lhs) && other.value.eq(&self.value)
    }
}
