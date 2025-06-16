import hashlib
from typing import Any, Dict, List, Set

from lark import Lark, ParseTree, Token, Transformer, Tree
from lark.reconstruct import Reconstructor
from pydantic_ai import Agent

from alphapower.constants import (
    FAST_EXPRESSION_GRAMMAR,
    USER_PROMPT_REGULAR_FAST_EXPRESSION_EXPLAIN,
    FastExpressionType,
)
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)

from .fast_expression_manager_abc import AbstractFastExpressionManager


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


class UsageCollector(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.assigned_vars: Set[str] = set()
        self.referenced_vars: Set[str] = set()
        self.used_operators: Set[str] = set()

    def assign(self, items: List[Any]) -> Any:
        varname = str(items[0])
        self.assigned_vars.add(varname)
        return items

    def variable(self, items: List[Any]) -> Any:
        varname = str(items[0])
        self.referenced_vars.add(varname)
        return items

    def function_call(self, items: List[Any]) -> Any:
        func_name = str(items[0])
        self.used_operators.add(func_name)
        return items

    def add(self, items: List[Any]) -> Any:
        self.used_operators.add("add")
        return items

    def sub(self, items: List[Any]) -> Any:
        self.used_operators.add("sub")
        return items

    def mul(self, items: List[Any]) -> Any:
        self.used_operators.add("mul")
        return items

    def div(self, items: List[Any]) -> Any:
        self.used_operators.add("div")
        return items

    def eq(self, items: List[Any]) -> Any:
        self.used_operators.add("eq")
        return items

    def ne(self, items: List[Any]) -> Any:
        self.used_operators.add("ne")
        return items

    def lt(self, items: List[Any]) -> Any:
        self.used_operators.add("lt")
        return items

    def le(self, items: List[Any]) -> Any:
        self.used_operators.add("le")
        return items

    def gt(self, items: List[Any]) -> Any:
        self.used_operators.add("gt")
        return items

    def ge(self, items: List[Any]) -> Any:
        self.used_operators.add("ge")
        return items


class FastExpressionNormalizer(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.assigned_vars: Set[str] = set()
        self.variable_count: int = 0

    def assign(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            self.assigned_vars.add(str(node))
        else:
            raise ValueError("Expected a Token for assignment.")
        return Tree("assign", items)

    def variable(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            if str(node) in self.assigned_vars:
                return Tree("variable", [node])

            token: Token = Token(node.type, "{" + f"var{self.variable_count}" + "}")
            self.variable_count += 1
            return Tree("variable", [token])
        else:
            raise ValueError("Expected a Token for variable.")

    def number(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            token: Token = Token("NAME", "{" + f"var{self.variable_count}" + "}")
            self.variable_count += 1
            return Tree("member", [Tree("variable", [token])])
        else:
            raise ValueError("Expected a Token for number.")

    def string(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            token: Token = Token("NAME", "{" + f"var{self.variable_count}" + "}")
            self.variable_count += 1
            return Tree("member", [Tree("variable", [token])])
        else:
            raise ValueError("Expected a Token for string.")

    def neg(self, items: List[Any]) -> Any:
        node: Any = items[0]
        if isinstance(node, Token):
            if node.type == "NUMBER":
                token: Token = Token("NAME", "{" + f"var{self.variable_count}" + "}")
                self.variable_count += 1
                return Tree("member", [Tree("variable", [token])])
        return Tree("neg", items)


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


class FastExpressionManager(BaseProcessSafeClass, AbstractFastExpressionManager):
    def __init__(
        self,
        agent: Agent,
    ) -> None:
        self.agent = agent

    def parse(self, expression: str, type: FastExpressionType) -> FastExpression:
        parser: Lark = Lark(
            FAST_EXPRESSION_GRAMMAR, parser="lalr", maybe_placeholders=False
        )
        ast_to_dict_transformer: Transformer = ASTToDict()
        normalizer: Transformer = FastExpressionNormalizer()
        reconstructor: Reconstructor = Reconstructor(parser)
        usage_collector: UsageCollector = UsageCollector()

        used_datafields: Set[str] = set()

        tree: ParseTree = parser.parse(expression)
        tree_dict: ParseTree = ast_to_dict_transformer.transform(tree)
        encoded_tree: str = tree_dict.pretty()
        fingerprint: str = hashlib.sha256(encoded_tree.encode()).hexdigest()

        usage_collector.transform(tree)
        used_datafields = (
            usage_collector.referenced_vars - usage_collector.assigned_vars
        )

        normalized_tree: ParseTree = normalizer.transform(tree)
        tree_dict = ast_to_dict_transformer.transform(normalized_tree)
        encoded_tree = tree_dict.pretty()
        structure_hash: str = hashlib.sha256(encoded_tree.encode()).hexdigest()
        normalized_expression: str = reconstructor.reconstruct(normalized_tree)

        return FastExpression(
            expression=expression,
            normalized_expression=normalized_expression,
            tree=tree,
            normalized_tree=normalized_tree,
            type=type,
            used_operators=usage_collector.used_operators,
            used_datafields=used_datafields,
            fingerprint=fingerprint,
            structure_hash=structure_hash,
        )

    async def explain(self, expression: FastExpression) -> str:
        """
        Explain the expression using the agent.
        """
        prompt = USER_PROMPT_REGULAR_FAST_EXPRESSION_EXPLAIN.format(
            expression=expression.normalized_expression
        )
        response = await self.agent.run(prompt)
        return response.output


class FastExpressionManagerFactory(BaseProcessSafeFactory):
    def __init__(self, agent: Agent) -> None:
        self.agent = agent

    async def _build(self, *args: Any, **kwargs: Any) -> AbstractFastExpressionManager:
        """
        Build a FastExpressionManager instance.
        """
        return FastExpressionManager(agent=self.agent)
