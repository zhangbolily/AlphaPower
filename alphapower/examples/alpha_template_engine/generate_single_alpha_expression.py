from alphapower.engine.simulation.template import (
    add,
    DataField,
    DataFieldType,
    Expression,
)

field1: DataField = DataField("field1", "Test Field", DataFieldType.MATRIX)
field2: DataField = DataField("field2", "Field 2", DataFieldType.MATRIX)

alpha_expr1: Expression = field1 + field2
alpha_expr2: Expression = add(field1, field2, filter=True)

for compiled in alpha_expr1.compile():
    print(compiled)

for compiled in alpha_expr2.compile():
    print(compiled)
