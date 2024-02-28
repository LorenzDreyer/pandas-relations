# Pandas-Relations

Pandas-Relations works on the pandas library to provide a more relational approach to data 
manipulation. The main purpose is to enable quick, easy and efficient data manipulation and
analysis.

## Disclaimer

Please note that this is a very, very early version of the package. Therefore, a lot of features are
missing and the package is not yet stable. I would really appreciate any feedback, suggestions or
contributions. In this way I know better what to focus on next and how to improve the package usage.
Thanks in advance!

## Installation

To be defined!

## Usage

Pandas-Relations tries to provide a as much as possible pandas-like experience. Therefore, all
the methods and attributes are similar to the ones in pandas. The only difference is in building
relations between dataframes and the way of filtering these relations. Let's get started:

```python
from pandas_relations import RelationalDataFrame as DataFrame

# some example data
customers_data = {
    "user_id": [1, 2, 3, 4],
    "name": ["Tom", "Jerry", "Mickey", "Donald"],
    "age": [30, 40, 50, 60],
    "country": ["USA", "UK", "France", "Germany"]
}

orders_data = {
    "order_id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
    "user_id": [1, 1, 2, 3, 3, 3, 4, 4, 4],
    "order_date": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07", "2021-01-08", "2021-01-09"],
    "amount": [100, 200, 300, 400, 500, 600, 700, 800, 900]
}

# build the dataframes
customers = DataFrame(customers_data)
orders = DataFrame(orders_data)

# the dataframes work the same as pandas dataframes
print(customers.head())
# Output:
#    user_id    name  age  country
# 0        1     Tom   30      USA
# 1        2   Jerry   40       UK
# 2        3  Mickey   50   France
# 3        4  Donald   60  Germany
```
After building the dataframes, we can build a relation between them. To connect the customers to the
orders, we can use the `relate` method.
    
```python
customers.relate(name="orders", dataframe=orders, on_left="user_id", on_right="user_id")
```
Now, the customers dataframe can use the orders dataframe for filtering.
    
```python
print(customers.rfilter("orders.amount > 500"))

# Output:
#    user_id    name  age  country
# 2        3  Mickey   50   France
# 3        4  Donald   60  Germany
```
The `rfilter` method filters the customers dataframe based on the orders dataframe. In this case, it
returns the customers who have made orders with an amount greater than 500.

The `rfilter` method is also able to filter based on multiple dataframes and multiple conditions.
```python
print(customers.rfilter("amount > 500 & age > 50"))
# Output:
#    user_id    name  age  country
# 3        4  Donald   60  Germany
```
As you can the the `rfilter` method does not even need to know the name of the dataframe. If the column
name is unique, it will automatically use the correct dataframe. If the column name is not unique, you
can specify the dataframe name by using the `dataframe.column` syntax. For a column in the own dataframe,
you never need to specify the dataframe name, but if you want to use the `self` keyword, you can do so.

```python
print(customers.rfilter("self.age > 50"))
# Output:
#    user_id    name  age  country
# 3        4  Donald   60  Germany
```

### Remarks

Some behavior you should be aware of:
- Currently, when using the `rfilter` method, the resulting dataframe is a standard pandas dataframe. The relations of the original dataframe are not copied.


## License

To be defined!
