from __future__ import annotations

from pandas import DataFrame

from .relational_filter import RelationalFilter


class RelationalDataFrame:
    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False):
        self.data = DataFrame(data, index, columns, dtype, copy)
        self._relations = {}
        self.filter = RelationalFilter(self)

    def __getattr__(self, name):
        return getattr(self.data, name)

    @property
    def _constructor(self):
        return RelationalDataFrame

    def relate(self, name: str, dataframe: 'RelationalDataFrame', on_left: str, on_right: str):
        self._relations[name] = {
            "dataframe": dataframe,
            "own_column": on_left,
            "other_column": on_right
        }

    def rfilter(self, query: str) -> 'RelationalDataFrame':
        """
        The relational filter method allows you to filter a dataframe easy based on its own columns and
        the columns of related dataframes. The query string should be a valid pandas query string.

        A single query contains up to 3 parts:
        - The left part of the query is the column name of the current dataframe
        - The middle part of the query is the operator
        - The right part is the condition to be met
        e.g. "age > 30" filters the dataframe to only include rows where the age is greater than 30.

        Evenly acceptable is a query where the left part is starting with the name of the dataframe
        or the self keyword.
        e.g. "customers.age > 30" or "self.age > 30" filters the dataframe to only include rows where
        the age is greater than 30.

        The true power ot the relational filter method comes when you do not only filter based on the
        columns of the current dataframe but also based on the columns of related dataframes. To do
        so, you can use the name of the related dataframe as the left part of the query.
        e.g. "orders.amount > 1000" filters the dataframe to only include rows where the amount of
        the related orders dataframe is greater than 1000.

        In case the query contains a column name that is unique in all the related dataframes, the
        name of the related dataframe can be omitted.
        e.g. "amount > 1000" filters the dataframe to only include rows where the amount of the
        related orders dataframe is greater than 1000.

        In case a column name is not unique in all the related dataframes, the name of the related
        dataframe must be used.

        Of course, also a combination of multiple queries using the logical operators & and | is possible.
        e.g. "age > 30 & orders.amount > 1000" filters the dataframe to only include rows where the age
        is greater than 30 and the amount of the related orders dataframe is greater than 1000.

        Also the use of brackets is possible.
        e.g. "(age > 30 | age < 20) & orders.amount > 1000" filters the dataframe to only include rows
        where the age is greater than 30 or less than 20 and the amount of the related orders dataframe
        is greater than 1000.

        :param query: The query string to filter the dataframe
        :return: A new dataframe containing only the rows that meet the condition
        """
        return self.filter.filter_data(query)
