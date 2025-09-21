import random
from collections import Counter

DUMMY_PRODUCTS = [
    {'id': '85227', 'distance': 0.15, 'metadata': {'name': 'Genesis 9 Starter Essentials', 'artist': 'Daz Originals', 'url': 'https://...', 'tags': 'genesis 9, character, essentials', 'last_updated': '2023-10-28T10:00:00Z'}},
    {'id': '91123', 'distance': 0.28, 'metadata': {'name': 'Cyberpunk Gunslinger Outfit', 'artist': 'Val-Z', 'url': 'https://...', 'tags': 'sci-fi, cyberpunk, outfit', 'last_updated': '2023-10-27T14:30:00Z'}},
    {'id': '88441', 'distance': 0.41, 'metadata': {'name': 'Modern Urban Street Environment', 'artist': 'Stonemason', 'url': 'https://...', 'tags': 'environment, city, urban', 'last_updated': '2023-10-26T09:15:00Z'}},
    {'id': '90567', 'distance': 0.55, 'metadata': {'name': 'Ancient Wizard Staffs', 'artist': 'MerlinProps', 'url': 'https://...', 'tags': 'fantasy, wizard, props', 'last_updated': '2023-10-25T11:00:00Z'}}
]

def get_demo_stats():
    all_tags = [tag.strip() for p in DUMMY_PRODUCTS for tag in p['metadata']['tags'].split(',')]
    return {'total_docs': len(DUMMY_PRODUCTS), 'last_update': max(p['metadata']['last_updated'] for p in DUMMY_PRODUCTS), 'tag_histogram': dict(Counter(all_tags))}

def get_demo_search_results(prompt, limit=10, offset=0, **kwargs):
    random.shuffle(DUMMY_PRODUCTS)
    paginated_results = DUMMY_PRODUCTS[offset : offset + limit]
    return {"total_hits": len(DUMMY_PRODUCTS), "limit": limit, "offset": offset, "results": paginated_results}