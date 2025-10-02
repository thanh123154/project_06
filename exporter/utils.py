import json
from typing import Any, Dict, Iterable, List, Optional


def to_string(value: Any) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False, default=str)
        return str(value)
    except Exception:
        return repr(value)


def flatten_dict(document: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    items: List[tuple] = []
    for key, value in document.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)


def sanitize_document(document: Dict[str, Any]) -> Dict[str, Any]:
    def default(o: Any) -> str:
        return to_string(o)

    def normalize_empty_values(obj: Any) -> Any:
        """Convert empty strings to empty arrays for array fields"""
        if isinstance(obj, dict):
            normalized = {}
            for key, value in obj.items():
                # Handle specific fields that should be arrays
                if key in ['cart_products', 'recommendation_products', 'recommendation_view_all_products']:
                    if value == "" or value is None:
                        normalized[key] = []
                    elif isinstance(value, list):
                        normalized[key] = [
                            normalize_empty_values(item) for item in value]
                    else:
                        normalized[key] = [normalize_empty_values(value)]
                elif isinstance(value, dict):
                    # Recursively normalize nested objects
                    normalized[key] = normalize_empty_values(value)
                elif isinstance(value, list):
                    normalized[key] = [
                        normalize_empty_values(item) for item in value]
                else:
                    normalized[key] = value
            return normalized
        elif isinstance(obj, list):
            return [normalize_empty_values(item) for item in obj]
        else:
            return obj

    def normalize_cart_products_option(obj: Any) -> Any:
        """Specifically handle cart_products.option field normalization"""
        if isinstance(obj, dict):
            normalized = {}
            for key, value in obj.items():
                if key == 'option':
                    # Ensure option is always an array
                    if value == "" or value is None:
                        normalized[key] = []
                    elif isinstance(value, list):
                        normalized[key] = value
                    else:
                        normalized[key] = [value]
                elif isinstance(value, dict):
                    normalized[key] = normalize_cart_products_option(value)
                elif isinstance(value, list):
                    normalized[key] = [
                        normalize_cart_products_option(item) for item in value]
                else:
                    normalized[key] = value
            return normalized
        elif isinstance(obj, list):
            return [normalize_cart_products_option(item) for item in obj]
        else:
            return obj

    try:
        # First normalize empty values
        normalized_doc = normalize_empty_values(document)
        # Then normalize cart_products.option specifically
        normalized_doc = normalize_cart_products_option(normalized_doc)
        # Finally sanitize
        dumped = json.loads(json.dumps(
            normalized_doc, default=default, ensure_ascii=False))
    except Exception:
        dumped = {k: to_string(v) for k, v in document.items()}
    return dumped


def load_query(query_json: Optional[str], query_file: Optional[str]) -> Dict[str, Any]:
    if query_json:
        return json.loads(query_json)
    if query_file:
        with open(query_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def iter_batches(cursor, batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
