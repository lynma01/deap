# Data Exploration & Analysis Platform

## Purpose
This package is to assist students & academics in creating and catalogging localized datalakes while making practical use of big-data technology.

## Problem
Given the massive amount of data, there's a fundamental problem of testing its cohesiveness. Whenever we as analysts receive a new dataset, besides analyzing the data itself, we must also understand how well it fits into our overall data catalog. This is a time-consuming effort that this package hopes to alleviate.

## Concepts
This package is a tool for analyzing the [cardinality - Wikipedia](https://en.wikipedia.org/wiki/Cardinality_%28data_modeling%29) of a previously unknown dataset. It does this by testing for the `type`, `key_strength`, and then `join_strength`of the different columns relative to other tables in the dataset.

### `type`
As defined within `polars.DataFrame.schema`, the `dtype` allows us to figure out whether the column-values could make an effective join target.

Potential Join Target:
- `str`
- `int`
- `date`



### `key_strength`