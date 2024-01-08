import datetime as dt
from typing import Dict, Sequence, Tuple, Union

Value = Union[str, int, float, dt.datetime, None]
Row = Dict[str, Value]

IdArg = Union[int, str, Sequence[Union[int, str]], None]

DateSpec = Union[
    str,
    dt.datetime,
    Tuple[
        Union[str, dt.datetime],
        Union[str, dt.datetime],
    ],
]
DateArg = Union[DateSpec, None]
