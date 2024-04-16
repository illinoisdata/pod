from __future__ import annotations  # isort:skip
import pod.__pickle__  # noqa, isort:skip

import ast
from types import ModuleType
from typing import Iterable

import numpy as np
import pandas as pd

from pod._pod import Namespace


class StaticCodeChecker:
    def is_static(self, code: str, namespace: Namespace) -> bool:
        raise NotImplementedError("Abstract method")


""" Always Non-static Code Checker (Disabled Checking) """


class AlwaysNonStaticCodeChecker(StaticCodeChecker):
    def __init__(self):
        pass

    def is_static(self, code: str, namespace: Namespace) -> bool:
        return False


""" Allow-list Static Code Checker """


DEFAULT_MATCH_RULES = [
    (ast.Attribute, Iterable, "max", None, None),
    (ast.Attribute, Iterable, "mean", None, None),
    (ast.Attribute, Iterable, "median", None, None),
    (ast.Attribute, Iterable, "min", None, None),
    (ast.Attribute, Iterable, "sum", None, None),
    (ast.Attribute, ModuleType, "listdir", None, None),
    (ast.Attribute, ModuleType, "max", None, None),
    (ast.Attribute, ModuleType, "mean", None, None),
    (ast.Attribute, ModuleType, "median", None, None),
    (ast.Attribute, ModuleType, "min", None, None),
    (ast.Attribute, ModuleType, "number_strongly_connected_components", None, None),
    (ast.Attribute, ModuleType, "number_weakly_connected_components", None, None),
    (ast.Attribute, ModuleType, "pivot_table", None, None),
    (ast.Attribute, ModuleType, "sum", None, None),
    (ast.Attribute, np.ndarray, "shape", None, None),
    (ast.Attribute, np.ndarray, "value_counts", None, None),
    (ast.Attribute, pd.DataFrame, "clip", {"inplace": True}, None),
    (ast.Attribute, pd.DataFrame, "corr", None, None),
    (ast.Attribute, pd.DataFrame, "duplicated", None, None),
    (ast.Attribute, pd.DataFrame, "groupby", None, None),
    (ast.Attribute, pd.DataFrame, "head", None, None),
    (ast.Attribute, pd.DataFrame, "hist", None, None),
    (ast.Attribute, pd.DataFrame, "iloc", None, None),
    (ast.Attribute, pd.DataFrame, "index", None, None),
    (ast.Attribute, pd.DataFrame, "info", None, None),
    (ast.Attribute, pd.DataFrame, "isna", None, None),
    (ast.Attribute, pd.DataFrame, "loc", None, None),
    (ast.Attribute, pd.DataFrame, "max", None, None),
    (ast.Attribute, pd.DataFrame, "mean", None, None),
    (ast.Attribute, pd.DataFrame, "median", None, None),
    (ast.Attribute, pd.DataFrame, "min", None, None),
    (ast.Attribute, pd.DataFrame, "plot", None, None),
    (ast.Attribute, pd.DataFrame, "sales", None, None),
    (ast.Attribute, pd.DataFrame, "set_index", {"inplace": True}, None),
    (ast.Attribute, pd.DataFrame, "sum", None, None),
    (ast.Attribute, pd.DataFrame, "tail", None, None),
    (ast.Attribute, pd.Series, "index", None, None),
    (ast.Attribute, pd.Series, "nunique", None, None),
    (ast.Attribute, pd.Series, "value_counts", None, None),
    (ast.Attribute, str, "format", None, None),
    (ast.Attribute, str, "join", None, None),
    (ast.Name, float, "round", None, None),
    (ast.Name, Iterable, "len", None, None),
    (ast.Name, None, "BEST_ROUNDS", None, None),
    (ast.Name, None, "hard_rows_list", None, None),
    (ast.Name, None, "print", None, None),
    (ast.Name, None, "str", None, None),
    (ast.Subscript, list, None, None, None),
    (ast.Subscript, pd.DataFrame, None, None, None),
    (ast.UnaryOp, int, ast.Not, None, None),
]


class AllowlistStaticCodeChecker(StaticCodeChecker):
    def __init__(self):
        pass

    def is_static(self, code: str, namespace: Namespace) -> bool:
        code_ast = ast.parse(code)
        static_checker = AllowlistVisitor(DEFAULT_MATCH_RULES, namespace)
        return static_checker.visit(code_ast, None)


class AllowlistVisitor(ast.NodeVisitor):
    DISALLOW = (
        ast.FunctionDef,
        ast.AsyncFunctionDef,
        ast.ClassDef,
        ast.Delete,
        ast.Assign,
        # ast.TypeAlias,
        ast.AugAssign,
        ast.AnnAssign,
        ast.For,
        ast.AsyncFor,
        ast.With,
        ast.AsyncWith,
        ast.Import,
        ast.ImportFrom,
        ast.Global,
        ast.Nonlocal,
        ast.NamedExpr,
    )

    MATCH = (ast.BoolOp, ast.BinOp, ast.UnaryOp, ast.Attribute, ast.Subscript, ast.Starred, ast.Name, ast.Slice, ast.ExtSlice)

    ALLOW = (
        ast.Module,
        ast.Interactive,
        ast.Expression,
        ast.Return,
        ast.While,
        ast.If,
        ast.Match,
        ast.Index,
        ast.Raise,
        ast.Try,
        # ast.TryStar,
        ast.Assert,
        ast.Expr,
        ast.Pass,
        ast.Break,
        ast.Continue,
        ast.Lambda,
        ast.IfExp,
        ast.Dict,
        ast.Set,
        ast.ListComp,
        ast.SetComp,
        ast.DictComp,
        ast.GeneratorExp,
        ast.Await,
        ast.Yield,
        ast.YieldFrom,
        ast.Compare,
        ast.Call,
        ast.FormattedValue,
        ast.JoinedStr,
        ast.Constant,
        ast.List,
        ast.Tuple,
        ast.Load,  # Context for load (read) operations
        ast.Store,  # Context for store (write) operations
        ast.Del,  # Context for delete operations
        ast.And,  # Boolean 'and'
        ast.Or,  # Boolean 'or'
        ast.Add,  # Addition operator
        ast.Sub,  # Subtraction operator
        ast.Mult,  # Multiplication operator
        ast.MatMult,  # Matrix multiplication operator
        ast.Div,  # Division operator
        ast.Mod,  # Modulus operator
        ast.Pow,  # Power operator
        ast.LShift,  # Left shift operator
        ast.RShift,  # Right shift operator
        ast.BitOr,  # Bitwise OR operator
        ast.BitXor,  # Bitwise XOR operator
        ast.BitAnd,  # Bitwise AND operator
        ast.FloorDiv,  # Floor division operator
        ast.Invert,  # Unary inversion operator
        ast.Not,  # Unary not operator
        ast.UAdd,  # Unary addition operator
        ast.USub,  # Unary subtraction operator
        ast.Eq,  # Equality operator
        ast.NotEq,  # Not equal operator
        ast.Lt,  # Less than operator
        ast.LtE,  # Less than or equal to operator
        ast.Gt,  # Greater than operator
        ast.GtE,  # Greater than or equal to operator
        ast.Is,  # Identity operator
        ast.IsNot,  # Non-identity operator
        ast.In,  # Membership operator
        ast.NotIn,  # Non-membership operator
        ast.ExceptHandler,  # Exception handler
        ast.comprehension,  # Comprehension structure
        ast.arguments,  # Function/method argument structure
        ast.arg,  # Single argument definition
        ast.keyword,  # Keyword in function calls
        ast.alias,  # Import alias
        ast.withitem,  # With statement item
        ast.match_case,
        ast.MatchValue,
        ast.MatchSingleton,
        ast.MatchSequence,
        ast.MatchMapping,
        ast.MatchClass,
        ast.MatchStar,
        ast.MatchAs,
        ast.MatchOr,
        # ast.ParamSpec,
        # ast.TypeVarTuple,
        ast.TypeIgnore,
    )

    def __init__(self, rules, namespace):
        self.rules = rules
        self.safe = set()
        self.namespace = namespace

    def visit(self, node, parent=None):
        if not self.is_static(node, parent):
            return False
        for field, value in ast.iter_fields(node):
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, ast.AST):
                        if not self.visit(item, node):
                            return False
            elif isinstance(value, ast.AST):
                if not self.visit(value, node):
                    return False
        return True

    def is_static(self, node, parent):
        if isinstance(node, AllowlistVisitor.DISALLOW):
            return False
        if isinstance(node, AllowlistVisitor.ALLOW):
            if isinstance(node, ast.ListComp) or isinstance(node, ast.DictComp) or isinstance(node, ast.SetComp):
                for item in node.generators:
                    if isinstance(item.target, ast.Name):
                        self.safe.add(item.target)
            return True
        if isinstance(node, AllowlistVisitor.MATCH):
            if self._match(node, parent):
                return True
        if self.final_checks(node, parent):
            return True
        return False

    def _match(self, node: ast.AST, parent: ast.AST):
        for rule in self.rules:
            node_type, var_type, value, disallow_args, need_args = rule
            if (
                node_type == ast.Attribute
                and isinstance(node, node_type)
                and node.attr == value
                and self.query_namespace(node.value, var_type, node)
            ):
                if isinstance(node.value, ast.Name):
                    self.safe.add(node.value)
                if isinstance(node.value, ast.Constant):
                    return True
                if isinstance(parent, ast.Call):
                    if not self._verify_fn(disallow_args, need_args, parent):
                        return False
                    for arg in parent.args:
                        if isinstance(arg, ast.Name):
                            self.safe.add(arg)
                return True
            elif node_type == ast.Name and isinstance(node, node_type) and node.id == value:
                if isinstance(parent, ast.Call):
                    if not self._verify_fn(disallow_args, need_args, parent):
                        return False
                    for arg in parent.args:
                        if isinstance(arg, ast.Name):
                            self.safe.add(arg)
                return True
            elif node_type == ast.Slice and isinstance(node, node_type):
                return True
            elif (
                node_type == ast.Subscript and isinstance(node, node_type) and self.query_namespace(node.value, var_type, node)
            ):
                slice_val = node.slice
                self.safe.add(slice_val)
                for field, value in ast.iter_fields(slice_val):
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, ast.AST):
                                for n in ast.walk(item):
                                    self.safe.add(n)
                    elif isinstance(value, ast.AST):
                        for n in ast.walk(value):
                            self.safe.add(n)
                if isinstance(node.value, ast.Name):
                    self.safe.add(node.value)
                return True
            elif (
                node_type == ast.UnaryOp
                and isinstance(node, node_type)
                and isinstance(node.op, value)
                and self.query_namespace(node.operand, var_type, node)
            ):
                if isinstance(node.operand, ast.Name):
                    self.safe.add(node.operand)
                return True

        if node in self.safe:
            return True
        return False

    def _verify_fn(self, disallow_args, need_args, parent):
        if disallow_args:
            for keyword in parent.keywords:
                for banned_key, banned_value in disallow_args.items():
                    if (
                        keyword.arg == banned_key
                        and isinstance(keyword.value, ast.Constant)
                        and keyword.value.value == banned_value
                    ):
                        return False
        if need_args:
            for needed_key, needed_value in need_args.items():
                has_arg = False
                for keyword in parent.keywords:
                    if (
                        keyword.arg == needed_key
                        and isinstance(keyword.value, ast.Constant)
                        and keyword.value.value == needed_value
                    ):
                        has_arg = True
                if not has_arg:
                    return False
        return True

    def final_checks(self, node, parent):
        if isinstance(node, ast.Name) and isinstance(parent, ast.Expr):
            return True
        if isinstance(node, ast.UnaryOp) and isinstance(node.operand, ast.Constant):
            return True
        return False

    def query_namespace(self, val, expected_type, node=None):
        if isinstance(val, ast.Constant):
            return True
        if not isinstance(val, ast.Name):
            for n in ast.walk(val):
                if isinstance(n, ast.Name):
                    val = n
                    break
            if not isinstance(val, ast.Name):
                raise Exception(f"No vals found {ast.dump(node)}")
        if val.id not in self.namespace:
            raise Exception("NOT IN NAMESPACE")
        if not isinstance(self.namespace[val.id], expected_type):
            # print(f"TYPE ISSUE {type(self.namespace[val.id])}, {expected_type}\nNODE = {ast.dump(node)}")
            return False
        return True


if __name__ == "__main__":
    with open("ast_lines.txt", "r") as line_file:
        for code in line_file.readlines():
            t = ast.parse(code)
            print(ast.dump(t))

            s = AllowlistVisitor(DEFAULT_MATCH_RULES, {})
            if not s.visit(t, None):
                break
