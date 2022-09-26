from typing import Any, Optional, TypeVar

NumberType = TypeVar("NumberType", int, float)


def _get_value_by_range(
    value: NumberType, gte: Optional[NumberType] = None, lte: Optional[NumberType] = None
) -> NumberType:
    if gte is not None and value < gte:
        value = gte
    elif lte is not None and value > lte:
        value = lte
    return value


def get_value_by_range(
    value: NumberType, gte: Optional[NumberType] = None, lte: Optional[NumberType] = None
) -> NumberType:
    if gte and lte and gte > lte:
        raise ValueError("gte must less than lte")
    return _get_value_by_range(value, gte, lte)


class NumberRange(object):
    def __init__(self, value: NumberType, gte: Optional[NumberType] = None, lte: Optional[NumberType] = None):
        """
        gte >=; lte <=
        """
        self.gte: Optional[NumberType] = gte
        self.lte: Optional[NumberType] = lte
        self.value: Any = get_value_by_range(value, gte, lte)

    def __set__(self, instance: Any, value: NumberType):
        self.value = _get_value_by_range(value, self.gte, self.lte)

    def __get__(self, instance: Any, owner: Any) -> NumberType:
        return self.value

    @classmethod
    def i(cls, value: NumberType, gte: Optional[NumberType] = None, lte: Optional[NumberType] = None) -> Any:
        return cls(value, gte, lte)
