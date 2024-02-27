import operator


class Operator:
    """
    This class is used to get the correct operator function based on the operator symbol.
    """
    operator_dict = {
        '<': operator.lt,
        '<=': operator.le,
        '==': operator.eq,
        '!=': operator.ne,
        '>=': operator.ge,
        '>': operator.gt
    }

    @staticmethod
    def get_operator(operator_symbol: str) -> callable:
        return Operator.operator_dict[operator_symbol]

    def __call__(self, operator_symbol: str) -> callable:
        return self.get_operator(operator_symbol)
