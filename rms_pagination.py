"""Complete OnRent reads with the previous item-reader visibility retained."""
from bridge_safety import RecoveryRequired


class ServiceListing(list):
    """Full service list; newly revealed untracked items are report-only in sync."""
    def __init__(self, rows, legacy_visible_ids):
        super().__init__(rows)
        self.legacy_visible_ids = frozenset(legacy_visible_ids)


def fetch_pages(client, url, headers, collection, params=None):
    rows, seen, legacy_ids = [], set(), set()
    expected_total = None
    legacy_open = True
    for page in range(1, 201):
        response = client.get(url, headers=headers,
                              params={**(params or {}), 'page': page, 'per_page': 100})
        response.raise_for_status()
        body = response.json()
        batch = body[collection]
        if not isinstance(batch, list):
            raise RecoveryRequired('Invalid OnRent pagination rows')
        meta = body.get('meta') or {}
        total = meta.get('total_row_count')
        if total is not None:
            if type(total) is not int or total < 0:
                raise RecoveryRequired('Invalid OnRent pagination total')
            if expected_total is not None and total != expected_total:
                raise RecoveryRequired('OnRent pagination total changed during scan')
            expected_total = total
        ids = [row['id'] for row in batch]
        if len(set(ids)) != len(ids) or seen.intersection(ids):
            raise RecoveryRequired('OnRent pagination did not advance cleanly')
        if legacy_open:
            legacy_ids.update(ids)
            # Exactly the deployed item reader's stopping rule. Retain its
            # visibility only as a write gate, never as completeness evidence.
            legacy_page_size = meta.get('per_page', 100)
            if type(legacy_page_size) is not int or legacy_page_size <= 0:
                raise RecoveryRequired('Invalid OnRent pagination page size')
            if page * legacy_page_size >= meta.get('total_row_count', 0):
                legacy_open = False
        rows.extend(batch)
        seen.update(ids)
        if expected_total is not None:
            if len(rows) > expected_total:
                raise RecoveryRequired('OnRent pagination exceeded advertised total')
            if len(rows) == expected_total:
                return rows, legacy_ids
            if not batch:
                raise RecoveryRequired('OnRent pagination ended before advertised total')
        elif not batch:
            return rows, legacy_ids
    raise RecoveryRequired('OnRent pagination exceeded bounded scan')
