# %%
import polars as pl

# %%
r_data = pl.DataFrame(
    {
      "a": [1, 3, 4, 6, 7]
    , "id": [1, 2, 3, 4, 5] 
    }
)

l_data = pl.DataFrame(
    {
      "b": [1, 3, 4, 6, 7]
    , "id": [1, 2, 2, 3, 4]
    }
)

# %%
@pl.api.register_dataframe_namespace("join_strength")
class JoinStrength:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def get_common_columns(self, right_df: pl.DataFrame, strict: bool = True) -> list:
        """
        Returns a list of common columns between two `polars.DataFrame` objects.

        If `strict`, then only return columns with the same `polars.DataType`.
        """

        left_df_schema, right_df_schema = self._df.schema, right_df.schema
        common_columns = []

        if not strict:
            common_columns = set(left_df_schema.keys()) & set(right_df_schema.keys())
        elif strict:
            for i, j in zip(left_df_schema.items(), right_df_schema.items()):
                if i == j:
                    common_columns.append(i)
        
        return common_columns

    def test_join(self, right_df: pl.DataFrame, key_columns: list, join_validations: list = ["m:m", "m:1", "1:m", "1:1"], join_types: list = ["inner", "left", "outer", "semi", "anti", "cross", "outer_coalesce"]) -> pl.DataFrame:
        """
        Tests whether two `polars.DataFrame` objects can be joined with the specified join_types and validations.
        """

        ref_join_types = ["inner", "left", "outer", "semi", "anti", "cross", "outer_coalesce"]
        ref_join_validations = ["m:m", "m:1", "1:m", "1:1"]

        iter_join_types = (join_type for join_type in join_types if join_type in ref_join_types)

        iter_join_validations = (join_validation for join_validation in join_validations if join_validation in ref_join_validations)

        results = {}

        for join_type in iter_join_types:
            for join_validation in iter_join_validations:
                try:
                    self._df.join(other=right_df, on=key_columns, how=join_type, validate=join_validation)
                    results[join_type][join_validation] = True
                except:
                    results[join_type][join_validation] = False
        
        return results

# %%

r_data.join_strength.test_join(l_data, key_columns=["id"], join_types=["left"], join_validations=["m:m", "1:1"])
# %%





# %%
# %%

