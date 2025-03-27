from .core import Expression


class FunctionLibrary:
    _registry = {}

    @classmethod
    def register(cls, name: str, func):
        cls._registry[name] = func

    @classmethod
    def get(cls, name: str):
        return cls._registry.get(name)


class ExpressionValidator:
    def validate(self, expression: Expression):
        if expression.op == "ts_mean":
            assert isinstance(expression.params.get("d"), int), "参数d必须是整数"
            assert expression.params["d"] > 0, "时间窗口必须为正"
        # Add more validation rules as needed
