from unidecode import unidecode


def clean_text(text):
    if text is None:
        return None
    return unidecode(text.strip())
