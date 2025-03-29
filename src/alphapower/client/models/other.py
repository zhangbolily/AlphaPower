import json


class Operator:
    def __init__(
        self, name, category, scope, definition, description, documentation, level
    ):
        self.name = name
        self.category = category
        self.scope = scope
        self.definition = definition
        self.description = description
        self.documentation = documentation
        self.level = level


class Operators:
    def __init__(self, operators):
        self.operators = [Operator(**operator) for operator in operators]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(data)
