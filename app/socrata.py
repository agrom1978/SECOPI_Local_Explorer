import requests
from typing import Dict, Any, Iterator, List, Optional

class SocrataClient:
    def __init__(self, domain: str, app_token: Optional[str] = None,
                 username: Optional[str] = None, password: Optional[str] = None,
                 timeout: int = 60):
        self.base = f"https://{domain}"
        self.session = requests.Session()
        self.timeout = timeout
        if app_token:
            self.session.headers.update({"X-App-Token": app_token})
        if username and password:
            self.session.auth = (username, password)

    def fetch_page(self, dataset_id: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        url = f"{self.base}/resource/{dataset_id}.json"
        r = self.session.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def iter_query(self, dataset_id: str, select: str, where: Optional[str],
                   order: Optional[str], limit: int = 50000) -> Iterator[List[Dict[str, Any]]]:
        offset = 0
        while True:
            params = {"$select": select, "$limit": limit, "$offset": offset}
            if where:
                params["$where"] = where
            if order:
                params["$order"] = order
            batch = self.fetch_page(dataset_id, params)
            if not batch:
                break
            yield batch
            offset += limit
