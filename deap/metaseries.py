# %%
import polars as pl

# %%
@pl.api.register_series_namespace("uniqueness")
class Uniqueness:
    def __init__(self, s: pl.Series):
        self._s = s

    def percent_unique(self, strict: bool = False, ndigits: int = 4) -> float:

        def _calc_percent_unique(calc_series: pl.Series) -> float:
            return calc_series.n_unique() / calc_series.len()

        if not strict:
            return round(_calc_percent_unique(calc_series=self._s), ndigits=ndigits)

        elif strict and self._s.dtype() not in (pl.Unknown, pl.Struct, pl.Array):
            raise ValueError(f"Series is of type {self._s.dtype()} and this calculation")


# %%
test_series = pl.Series("a", [1, 2, 2, 3, 4, 5])
# %%
test_series.uniqueness.percent_unique()
# %%
