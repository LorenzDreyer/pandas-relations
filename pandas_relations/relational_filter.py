from __future__ import annotations

from typing import TYPE_CHECKING

from pandas import Series, NA, NaT

from .operator import Operator
from .query_parser import QueryParser

if TYPE_CHECKING:
    from . import RelationalDataFrame


class RelationalFilter:
    """
    This class is used to filter a RelationalDataFrame based on a query string.

    The queries are full strings that can contain multiple comparisons and logical operators.
    The comparisons (<, >, ==, !=, <=, >=) are separated by the logical operators & (and) and | (or).
    To solve the query correctly, the priority of brackets is taken into account. Therefore, the
    query string can contain brackets to group comparisons and logical operators.
    """
    def __init__(self, dataframe: 'RelationalDataFrame'):
        self.dataframe = dataframe

    def filter_data(self, query: str) -> 'RelationalDataFrame':
        """
        This method is the entry point to filter the dataframe based on a query string.

        It applies the four following steps to solve the query:
            1. Parse the query string
            2. Solve the single comparisons operations
            3. Combine the results by using the logical operators
            4. Apply the boolean mask to the dataframe rows

        :param query: The query string to filter the dataframe
        :return: A new dataframe containing only the rows that meet the condition
        """
        single_queries = QueryParser.parse_expr(query)
        solved_comparisons = self.solve_comparisons(single_queries)
        solved_query = self.solve_logical_operations(solved_comparisons)
        return self.dataframe.data[solved_query]

    # Step 2
    def solve_comparisons(self, layer: list[str | list]) -> list[Series | str | list]:
        """
        This method solves all the comparisons in the layer and returns a list of the results
        with the same structure.

        This means basically that the method will call the solve_comparison_string method for
        each comparison in the layer and the solve_query_layer method for each nested list in the layer.

        An example of a layer could be:
        [
            "age > 30",
            "&",
            [
                "orders.amount > 1000",
                "|",
                "orders.amount < 100"
            ]
        ]
        This example contains 2 layers (lists) and 3 comparisons (strings) and 2 logical operators (strings).

        The goal of this method is to solve all the comparisons while keeping the structure of the layer to
        ensure mathematical correctness.

        Output:
        [
            Series([True, False, True, ...]),
            "&",
            [
                Series([True, False, True, ...]),
                "|",
                Series([True, False, True, ...])
            ]
        ]

        :param layer: The current layer to solve
        :return: A list of the comparison results with the same logical structure
        """
        solved_comparisons = []
        if all(isinstance(i, str) for i in layer):
            solved_comparisons.append(self.solve_comparison_string(layer))  # single comparison
        else:
            for element in layer:
                if isinstance(element, list):
                    solved_comparisons.append(self.solve_comparisons(element))  # nested list
                else:
                    solved_comparisons.append(element)  # logical operator (& or |)
        return solved_comparisons

    # Step 2
    def solve_comparison_string(self, comparison: list[str, str, str]) -> Series:
        """
        This method solves a single comparison and returns the result as a boolean series.

        The comparison is a list of 3 strings in the following order:
        - The left part of the comparison (dataframe column)
        - The operator of the comparison
        - The left part of the comparison (comparison value)

        The method will first turn the strings into the correct data types and then solve the comparison.
        """
        series = self.identify_column(comparison[0].strip())
        operator_call = Operator.get_operator(comparison[1].strip())
        value = self.resolve_value_data_type(comparison[2].strip())

        if isinstance(series, tuple):
            return self.solve_relational_comparison(series[0], series[1], operator_call, value)
        return operator_call(series, value)

    # Step 2
    def solve_relational_comparison(self, relation: dict, column: str, operator_call: callable, value: any) -> Series:
        """
        In case a comparison is made on a related dataframe, this method is called to solve it and
        apply the result to the current dataframe.

        The relational comparison consists of four parts:
            1. Solve the comparison on the related dataframe.
            2. Get the ids of the key row that meet the condition of the comparison.
            3. Filter the current dataframe based on the key row ids.
            4. Return the boolean series of the filtered dataframe.

        :param relation: The relation entry of the related dataframe for the column to filter.
        :param column: The column name of the related dataframe to filter.
        :param operator_call: The operator to use for the comparison.
        :param value: The value to compare the column with.
        :return: A boolean series of the current dataframe.
        """
        other_df = relation["dataframe"].data
        relation_ids = other_df[operator_call(other_df[column], value)][relation["other_column"]].unique()
        return self.dataframe.data[relation["own_column"]].isin(relation_ids)

    # Step 2
    def identify_column(self, column_name: str) -> Series | tuple[dict, str]:
        """
        This method is used to identify the column of the dataframe the string is referring to.

        Currently, two possible cases are handled:
            1. Only column name is given (e.g. "age")
            2. The dataframe name and the column name are given (e.g. "customers.age")

        In case the dataframe name is `self`, the method will return the column of the current dataframe.

        If only the column name is given, and it is not known in the current dataframe, the method will
        search for the column in the related dataframes and check its uniqueness.

        :param column_name: The name of the column to identify.
        :return: The column of the dataframe the string is referring to.
        """
        if "." in column_name:
            table_name, column_name = column_name.split(".")
            if table_name == "self":
                return self.dataframe.data[column_name]
            else:
                return self.get_relation_data_column(table_name, column_name)
        else:
            if column_name in self.dataframe.data.columns:
                return self.dataframe.data[column_name]
            else:
                return self.find_column_in_relations(column_name)

    # Step 2
    def get_relation_data_column(self, table_name: str, column_name: str) -> tuple[str | None, str]:
        """
        This method is used to identify the column of a specific related dataframe.

        :param table_name: The dataframe name to search for the column.
        :param column_name: The column name to search for.
        :return: The column of the related dataframe the string is referring to.
        """
        relation_entry = self.dataframe._relations.get(table_name, None)
        if relation_entry is None:
            raise ValueError(f"Could not find a relation with the name '{table_name}'")
        elif column_name not in relation_entry["dataframe"].data.columns:
            raise ValueError(f"Could not find a column with the name '{column_name}' in the relation '{table_name}'")
        else:
            return relation_entry, column_name

    # Step 2
    def find_column_in_relations(self, column_name: str) -> tuple[str, str]:
        """
        This method is used to find a column in the related dataframes.
        Also checks if the column name is unique in all the related dataframes.

        :param column_name: The column name to search for.
        :return: The column of the related dataframe the string is referring to.
        """
        found_in_relation = None
        for relation in self._relations:
            if column_name in relation.data.columns:
                if found_in_relation is not None:
                    raise ValueError(f"Column name '{column_name}' is ambiguous")
                found_in_relation = relation

        if found_in_relation is None:
            raise ValueError(f"Could not find a column with the name '{column_name}'")
        return found_in_relation, column_name

    # Step 2
    @staticmethod
    def resolve_value_data_type(value: str) -> any:
        """
        This method is used to resolve the data type of the comparison value (right part of the comparison).

        TODO: add datetime support (isoformat)

        :param value: The value to resolve the data type of.
        :return: The value with the correct data type.
        """
        if (value[0] == "'" and value[-1] == "'") or (value[0] == '"' and value[-1] == '"'):
            return value[1:-1]
        elif value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        elif value.lower() == "none":
            return None
        elif value.lower() == "nan":
            return NA
        elif value.lower() == "nat":
            return NaT
        elif value.isdigit():
            return int(value)
        elif value.replace(".", "", 1).isdigit():
            return float(value)
        else:
            raise ValueError(f"Could not resolve the data type of the value '{value}'")

    # Step 3
    def solve_logical_operations(self, logical_layer: list) -> Series:
        """
        This method solves the logical operations in the query and returns the result as a boolean series.

        The logical operations are the & (and) and | (or) operators.
        Like in the `solve_comparisons` method, the method has to respect the structure of the layer
        to ensure mathematical correctness. Therefore, the method starts by solving the most nested
        logical operations first and then works its way up to the highest layer.

        :param logical_layer: The current layer to solve.
        :return: A boolean series of the logical operations.
        """
        if any(isinstance(i, list) for i in logical_layer):
            # If there are any nested lists in this layer, recurse into them first
            for i, element in enumerate(logical_layer):
                if isinstance(element, list):  # If the element is a list
                    logical_layer[i] = self.solve_logical_operations(element)
        else:
            # If we're in a layer with no more nested lists, this is the most deep-nested list.
            # Perform the boolean operations here.
            result = self.solve_single_logical_operation_layer(logical_layer)
            return result

        # After we've handled any nested lists in this layer, we can perform the boolean operation of the highest layer.
        result = self.solve_single_logical_operation_layer(logical_layer)
        return result

    # Step 3
    @staticmethod
    def solve_single_logical_operation_layer(condition_layer):
        """
        This method solves a single layer of logical operations and returns the result as a boolean series.

        :param condition_layer: The current layer to solve.
        :return: A boolean series of the logical operations.
        """
        result = condition_layer[0]  # Start with the first boolean series
        for i in range(1, len(condition_layer), 2):  # Iterate over the rest of the list with a step of 2
            if condition_layer[i] == '&':
                result = result & condition_layer[i + 1]
            elif condition_layer[i] == '|':
                result = result | condition_layer[i + 1]
            else:
                raise ValueError(f"Unknown logical operator '{condition_layer[i]}'")
        return result