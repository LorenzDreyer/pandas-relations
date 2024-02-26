import pyparsing as pp


class QueryParser:
    # Define literals for logical operators
    AND = pp.Keyword("&")
    OR = pp.Keyword("|")

    # Define the components of a condition
    identifier = pp.Word(pp.alphanums + "._")
    comparison_operator = pp.oneOf("> < >= <= == !=")
    value = pp.Word(pp.alphanums + "-.,' ")

    # Define structured elements of our language
    condition = pp.Group(
        identifier.setResultsName('identifier') +
        comparison_operator.setResultsName('comparison_operator') +
        value.setResultsName('value')
    )

    sub_expr = pp.Forward()

    inner_expr = pp.Group(
        (condition | (pp.Suppress("(") + sub_expr + pp.Suppress(")"))) +
        pp.ZeroOrMore(
            (AND | OR).setResultsName('logical_operator') +
            (condition | (pp.Suppress("(") + sub_expr + pp.Suppress(")")))
        )
    )

    sub_expr << inner_expr

    @classmethod
    def parse_expr(cls, query: str) -> list:
        parsed_query = cls.sub_expr.parseString(query, parseAll=True).asList()
        return cls.flatten_expression(parsed_query)

    @classmethod
    def flatten_expression(cls, lst: list) -> list:
        if len(lst) == 1 and isinstance(lst[0], list):
            return cls.flatten_expression(lst[0])
        else:
            return [cls.flatten_expression(i) if isinstance(i, list) else i for i in lst]
