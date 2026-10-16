"""Exact ownership evidence for the two journaled Sandalford arrival paragraphs."""
import re


def arrival_variants(operation):
    """Only the single paragraph appended by the reviewed operation is owned."""
    if operation.get('kind') != 'update':
        return ()
    before = operation.get('before', {}).get('notes', [])
    after = (operation.get('after') or {}).get('notes', [])
    if len(after) != len(before) + 1 or after[:-1] != before:
        return ()
    html = after[-1].get('html', '')
    if not re.fullmatch(
        r'<p data-rms-bridge-arrival="1">On site at (?:[1-9]|1[0-2]):[0-5][0-9](?:am|pm) sharp '
        r'\(AWST\)\. The one-minute slot represents arrival only, not work duration\.</p>', html
    ):
        return ()
    return (html, html.replace(' data-rms-bridge-arrival="1"', '', 1))



def visible_arrival_html(key, arrival):
    return f'<p>Bridge arrival (opportunity item #{key}): {arrival}</p>'
