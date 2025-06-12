import hashlib
from typing import Any, Dict, List, Set

from lark import Lark, ParseTree, Token, Transformer, Tree
from lark.reconstruct import Reconstructor

from alphapower.constants import FAST_EXPRESSION_GRAMMAR, FastExpressionType

from .fast_expression_manager_abc import FastExpressionManagerABC


class FastExpression:
    def __init__(
        self,
        expression: str,
        normalized_expression: str,
        tree: ParseTree,
        normalized_tree: ParseTree,
        type: FastExpressionType,
        used_operators: Set[str],
        used_datafields: Set[str],
        fingerprint: str,
        structure_hash: str,
    ) -> None:
        self.tree: ParseTree = tree
        self.normalized_tree: ParseTree = normalized_tree
        self.expression: str = expression
        self.normalized_expression: str = normalized_expression
        self.type: FastExpressionType = type
        self.used_operators: Set[str] = used_operators
        self.used_datafields: Set[str] = used_datafields
        self.fingerprint: str = fingerprint
        self.structure_hash: str = structure_hash

    def __repr__(self) -> str:
        return f"FastExpression(expression={self.expression}, type={self.type})"


class SystemVariableCollector(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.assigned_vars: Set[str] = set()
        self.referenced_vars: Set[str] = set()

    def assign(self, items: List[Any]) -> Any:
        varname = str(items[0])
        self.assigned_vars.add(varname)
        return items

    def variable(self, items: List[Any]) -> Any:
        varname = str(items[0])
        self.referenced_vars.add(varname)
        return items


class FastExpressionNormalizer(Transformer):
    def __init__(self) -> None:
        super().__init__()

    def assign(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            items[0] = Token(node.type, "var")
        return Tree("assign", items)

    def variable(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            items[0] = Token(node.type, "var")
        return Tree("variable", items)

    def number(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            items[0] = Token(node.type, "0")
        return Tree("number", items)

    def string(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            items[0] = Token(node.type, '"const"')
        return Tree("string", items)

    def neg(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            items[0] = Token(node.type, "0")
        return Tree("neg", items)


class NormalizedTreeCodeGenerator(Transformer):
    """
    将归一化后的 Lark ParseTree 反编译为表达式字符串。
    用于与 FastExpressionNormalizer 配套，支持 FAST_EXPRESSION_GRAMMAR 语法。
    """

    def statement_list(self, items: List[Any]) -> List[str]:
        return items

    def assign(self, items: List[Any]) -> str:
        # 赋值语句
        return f"{items[0]} = {items[1]}"

    def or_expr(self, items: List[Any]) -> str:
        return f"{items[0]} || {items[1]}"

    def and_expr(self, items: List[Any]) -> str:
        return f"{items[0]} && {items[1]}"

    def eq(self, items: List[Any]) -> str:
        return f"{items[0]} == {items[1]}"

    def ne(self, items: List[Any]) -> str:
        return f"{items[0]} != {items[1]}"

    def gt(self, items: List[Any]) -> str:
        return f"{items[0]} > {items[1]}"

    def lt(self, items: List[Any]) -> str:
        return f"{items[0]} < {items[1]}"

    def ge(self, items: List[Any]) -> str:
        return f"{items[0]} >= {items[1]}"

    def le(self, items: List[Any]) -> str:
        return f"{items[0]} <= {items[1]}"

    def add(self, items: List[Any]) -> str:
        return f"{items[0]} + {items[1]}"

    def sub(self, items: List[Any]) -> str:
        return f"{items[0]} - {items[1]}"

    def mul(self, items: List[Any]) -> str:
        return f"{items[0]} * {items[1]}"

    def div(self, items: List[Any]) -> str:
        return f"{items[0]} / {items[1]}"

    def neg(self, items: List[Any]) -> str:
        return f"-{items[0]}"

    def number(self, items: List[Any]) -> str:
        # 归一化后 number 只会是 __CONST__
        return str(items[0])

    def string(self, items: List[Any]) -> str:
        # 归一化后 string 只会是 __CONST__
        return str(items[0])

    def variable(self, items: List[Any]) -> str:
        # 归一化后 variable 只会是 __VAR__
        return str(items[0])

    def member(self, items: List[Any]) -> str:
        # 递归拼接成员访问
        base = items[0]
        for attr in items[1:]:
            base = f"{base}.{attr}"
        return base

    def positional_arg(self, items: List[Any]) -> str:
        return items[0]

    def keyword_arg(self, items: List[Any]) -> str:
        # 关键字参数格式：key=val
        return f"{items[0]}={items[1]}"

    def function_call(self, items: List[Any]) -> str:
        name = items[0]
        args = []
        for item in items[1:]:
            if isinstance(item, list):
                args.extend(item)
            else:
                args.append(item)
        return f"{name}({', '.join(args)})"

    def base(self, items: List[Any]) -> str:
        return items[0]

    def __default_token__(self, token: Token) -> str:
        return str(token)

    def __default__(self, data: Any, children: Any, meta: Any) -> Any:
        # 兜底处理
        if isinstance(children, list) and len(children) == 1:
            return children[0]
        return children


class ASTToDict(Transformer):
    def assign(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "assign", "target": str(items[0]), "value": items[1]}

    def add(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "add", "left": items[0], "right": items[1]}

    def sub(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "sub", "left": items[0], "right": items[1]}

    def mul(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "mul", "left": items[0], "right": items[1]}

    def div(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "div", "left": items[0], "right": items[1]}

    def eq(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "eq", "left": items[0], "right": items[1]}

    def ne(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "ne", "left": items[0], "right": items[1]}

    def lt(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "lt", "left": items[0], "right": items[1]}

    def le(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "le", "left": items[0], "right": items[1]}

    def gt(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "gt", "left": items[0], "right": items[1]}

    def ge(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "ge", "left": items[0], "right": items[1]}

    def and_expr(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "and", "left": items[0], "right": items[1]}

    def or_expr(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "or", "left": items[0], "right": items[1]}

    def neg(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "neg", "value": items[0]}

    def number(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "number", "value": float(items[0])}

    def string(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "string", "value": str(items[0])[1:-1]}

    def variable(self, items: List[Any]) -> Dict[str, Any]:
        return {"type": "variable", "name": str(items[0])}

    def member(self, items: List[Any]) -> Dict[str, Any]:
        base = items[0]
        for attr in items[1:]:
            base = {"type": "member", "object": base, "attr": str(attr)}
        return base

    def positional_arg(self, items: List[Any]) -> Dict[str, Any]:
        return items[0]

    def keyword_arg(self, items: List[Any]) -> Dict[str, Any]:
        return {"arg_type": "kw", "key": str(items[0]), "value": items[1]}

    def function_call(self, items: List[Any]) -> Dict[str, Any]:
        name = str(items[0])
        args = []
        kwargs = {}
        for item in items[1:]:
            if isinstance(item, dict) and item.get("arg_type") == "kw":
                kwargs[item["key"]] = item["value"]
            else:
                args.append(item)
        return {"type": "call", "func": name, "args": args, "kwargs": kwargs}

    def statement_list(self, items: List[Any]) -> List[Dict[str, Any]]:
        return items


class FastExpressionManager(FastExpressionManagerABC):
    def __init__(self) -> None:
        self.parser = Lark(
            FAST_EXPRESSION_GRAMMAR, parser="lalr", maybe_placeholders=False
        )
        self.reconstructor = Reconstructor(self.parser)
        self.system_variable_collector = SystemVariableCollector()
        self.normalizer = FastExpressionNormalizer()
        self.ast_to_dict = ASTToDict()
        self.normalized_tree_code_generator = NormalizedTreeCodeGenerator()

    def parse(self, expression: str, type: FastExpressionType) -> FastExpression:
        used_operators: Set[str] = set()
        used_datafields: Set[str] = set()

        tree: ParseTree = self.parser.parse(expression)
        tree_dict = self.ast_to_dict.transform(tree)
        encoded_tree: str = tree_dict.pretty()
        fingerprint: str = hashlib.sha256(encoded_tree.encode()).hexdigest()

        self.system_variable_collector.transform(tree)
        used_datafields = (
            self.system_variable_collector.referenced_vars
            - self.system_variable_collector.assigned_vars
        )

        normalized_tree: ParseTree = self.normalizer.transform(tree)
        tree_dict = self.ast_to_dict.transform(normalized_tree)
        encoded_tree = tree_dict.pretty()
        structure_hash: str = hashlib.sha256(encoded_tree.encode()).hexdigest()

        expression = self.reconstructor.reconstruct(tree)
        normalized_expression: str = self.reconstructor.reconstruct(normalized_tree)
        parser: Lark = Lark(
            FAST_EXPRESSION_GRAMMAR, parser="lalr", maybe_placeholders=False
        )
        reconstructor: Reconstructor = Reconstructor(parser)
        normalized_expression = reconstructor.reconstruct(normalized_tree)

        return FastExpression(
            expression=expression,
            normalized_expression=normalized_expression,
            tree=tree,
            normalized_tree=normalized_tree,
            type=type,
            used_operators=used_operators,
            used_datafields=used_datafields,
            fingerprint=fingerprint,
            structure_hash=structure_hash,
        )
