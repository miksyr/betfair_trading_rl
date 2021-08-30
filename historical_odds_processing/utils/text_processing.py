from typing import Union

from unidecode import unidecode


def clean_text(text: str) -> Union[str, None]:
    if text is None:
        return None
    return unidecode(text.strip())
