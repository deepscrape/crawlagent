import asyncio
import json
from typing import Any, Dict, List, Optional

from crawl4ai import AsyncUrlSeeder, SeedingConfig
from pydantic import BaseModel, Field
from pygments import highlight
from pygments.formatters import Terminal256Formatter
from pygments.lexers import JsonLexer

from schemas import CrawlOperation


class SeedingConfigModel(BaseModel):
    source: str = "sitemap+cc"
    pattern: Optional[str] = "*"
    live_check: Optional[bool] = False
    extract_head: Optional[bool] = False
    max_urls: Optional[int] = -1
    concurrency: Optional[int] = 1000
    hits_per_sec: Optional[int] = 5
    force: Optional[bool] = False
    base_directory: Optional[str] = None
    verbose: Optional[bool] = None
    query: Optional[List[str]] = None
    score_threshold: Optional[float] = None
    scoring_method: Optional[str] = "bm25"
    filter_nonsense_urls: Optional[bool] = True

# --- Pydantic Models for Requests ---
class SeederRequest(BaseModel):
    domains: List[str] = Field(..., description="Domain to discover URLs for")
    config: SeedingConfigModel = Field(..., description="SeedingConfig parameters")
    operation_data: CrawlOperation = Field(..., description="Crawl operation data")
    stream: Optional[bool] = Field(False, description="Stream results as they arrive")




# --- Helper: Build SeedingConfig from dict ---
def build_seeding_config(cfg: Dict[str, Any]) -> SeedingConfig:
    return SeedingConfig.from_kwargs(cfg)


### Multi-Domain Discovery
async def multi_domain_research(domains, cfg: Dict[str, Any]):
    async with AsyncUrlSeeder() as seeder:
        all_results = []
        # Research Python tutorials across multiple sites
        # domains = [
        #     "docs.python.org",
        #     "realpython.com",
        #     "python-course.eu",
        #     "tutorialspoint.com"
        # ]

        cfg['extract_head'] = True  # Required for content-based scoring
        cfg['scoring_method'] = "bm25"
        cfg['live_check'] = False

        queries = cfg.get("query", [])

        configs = [
            SeedingConfig.from_kwargs({**cfg, "query": query})
            for query in queries
        ]
            # source="sitemap",
            # extract_head=True,
            # query="python beginner tutorial basics",
            # scoring_method="bm25",
            # score_threshold=0.3,
            # max_urls=15, # Per domain
            # verbose=True,
    
        
        try:
            # Discover across all domains in parallel
            tasks = [
                seeder.many_urls(domains, config)
                for config in configs
            ]
            results_list = await asyncio.gather(*tasks)
            # Collect and rank all tutorials
            for query, results in zip(queries, results_list, strict=True):
                for domain, urls in results.items():
                    for url in urls:
                        url['domain'] = domain
                        url['query'] = query
                        all_results.append(url)
            
            
            # Sort by relevance across all domains
            # or no sort
            all_results.sort(key=lambda x: x['relevance_score'], reverse=True)
            
            # print(f"Top 10 Python tutorials across {len(domains)} sites:")
            # for i, tutorial in enumerate(all_results[:10], 1):
            #     score = tutorial['relevance_score']
            #     title = tutorial['head_data'].get('title', 'No title')[:60]
            #     domain = tutorial['domain']
            #     print(f"{i:2d}. [{score:.2f}] {title}")
            #     print(f"     {domain}")
            #     print(f"     {tutorial['url']}")

            return all_results
        
        except Exception as e:
            print(f"Error during multi-domain research: {e}")
            raise e
    

# Combined validation and metadata
async def comprehensive_validation():
    seeder = AsyncUrlSeeder()
    config = build_seeding_config(
        {
            "live_check": True,
            "extract_head": True,
            "max_urls": 50,
            "concurrency": 10,
            "scoring_method": "bm25",
            "query": "tutorial",
            "score_threshold": 0.2,
        }
    ) # Fill with desired parameters
    
    urls = await seeder.urls("scrapegraphai.com", config)
    
    # Filter for valid, relevant tutorials
    good_tutorials = [
        url for url in urls 
        if url['status'] == 'valid' and 
           url['relevance_score'] > 0.3 and
           'tutorial' in url['head_data'].get('title', '').lower()
    ]
    
    print(f"Found {len(good_tutorials)} high-quality tutorials:")
    json_str = json.dumps(good_tutorials, indent=2, ensure_ascii=False)
    print(highlight(json_str, JsonLexer(), Terminal256Formatter()))


# URL pattern filtering
patterns = [
    SeedingConfig(pattern="*/blog/*"),           # Blog posts only
    SeedingConfig(pattern="*.html"),             # HTML files only
    SeedingConfig(pattern="*/product/*"),        # Product pages
    SeedingConfig(pattern="*/docs/api/*"),       # API documentation
    SeedingConfig(pattern="*"),                  # Everything
]

# Advanced pattern usage
async def pattern_filtering():
    async with AsyncUrlSeeder() as seeder:
        # Find all blog posts from 2024
        config = SeedingConfig(
            source="sitemap",
            pattern="*/blog/2024/*.html",
            max_urls=100
        )
        
        blog_urls = await seeder.urls("example.com", config)
        
        # Further filter by keywords in URL
        python_posts = [
            url for url in blog_urls 
            if "python" in url['url'].lower()
        ]
        
        print(f"Found {len(python_posts)} Python blog posts")

# Performance optimization
async def performance_tuning():
    async with AsyncUrlSeeder() as seeder:
        # High-performance configuration
        config = SeedingConfig(
            source="cc",
            concurrency=50,        # Many parallel workers
            hits_per_sec=20,       # High rate limit
            max_urls=10000,        # Large dataset
            extract_head=False,    # Skip metadata for speed
            filter_nonsense_urls=True  # Auto-filter utility URLs
        )
        
        import time
        start = time.time()
        urls = await seeder.urls("large-site.com", config)
        elapsed = time.time() - start
        
        print(f"Processed {len(urls)} URLs in {elapsed:.2f}s")
        print(f"Speed: {len(urls)/elapsed:.0f} URLs/second")

# Memory-safe processing for large domains
async def large_domain_processing(url: str = "huge-site.com", validation = False):
    async with AsyncUrlSeeder() as seeder:
        # Safe for domains with 1M+ URLs
        config = SeedingConfig(
            source="cc+sitemap",
            live_check=validation,  # Optional live validation
            concurrency=50,        # Bounded queue adapts to this
            max_urls=100000,       # Process in batches
            extract_head=False,    # Skip metadata for speed
            filter_nonsense_urls=True
        )
        
        # The seeder automatically manages memory by:
        # - Using bounded queues (prevents RAM spikes)
        # - Applying backpressure when queue is full
        # - Processing URLs as they're discovered
        urls = await seeder.urls(url, config)

        print(f"Discovered {len(urls)} URLs from")

        # print with colors
        json_str = json.dumps(urls[:20], indent=2, ensure_ascii=False)
        print(highlight(json_str, JsonLexer(), Terminal256Formatter()))


# Metadata extraction and analysis
async def metadata_extraction(domain="example.com", source="sitemap+cc", pattern= "*", max_urls=100):
    async with AsyncUrlSeeder() as seeder:
        config = SeedingConfig(
            source,
            live_check=True,       # Skip live validation for speed
            extract_head=True,        # Extract <head> metadata
            pattern=pattern,
            max_urls=max_urls
        )
        
        urls = await seeder.urls(domain, config)
        
        # Analyze extracted metadata
        for url in urls:
            head_data = url['head_data']
            print(f"\nURL: {url['url']}")
            print(f"Title: {head_data.get('title', 'No title')}")
            
            # Standard meta tags
            meta = head_data.get('meta', {})
            print(f"Description: {meta.get('description', 'N/A')}")
            print(f"Keywords: {meta.get('keywords', 'N/A')}")
            print(f"Author: {meta.get('author', 'N/A')}")
            
            # Open Graph data
            print(f"OG Image: {meta.get('og:image', 'N/A')}")
            print(f"OG Type: {meta.get('og:type', 'N/A')}")
            
            # JSON-LD structured data
            jsonld = head_data.get('jsonld', [])
            if jsonld:
                print(f"Structured data: {len(jsonld)} items")
                for item in jsonld[:2]:
                    if isinstance(item, dict):
                        print(f"  Type: {item.get('@type', 'Unknown')}")
                        print(f"  Name: {item.get('name', 'N/A')}")


# Filter by metadata
async def metadata_filtering():
    async with AsyncUrlSeeder() as seeder:
        config = SeedingConfig(
            source="sitemap",
            extract_head=True,
            max_urls=100
        )
        
        urls = await seeder.urls("news.example.com", config)
        
        # Filter by publication date (from JSON-LD)
        from datetime import datetime, timedelta
        recent_cutoff = datetime.now() - timedelta(days=7)
        
        recent_articles = []
        for url in urls:
            for jsonld in url['head_data'].get('jsonld', []):
                if isinstance(jsonld, dict) and 'datePublished' in jsonld:
                    try:
                        pub_date = datetime.fromisoformat(
                            jsonld['datePublished'].replace('Z', '+00:00')
                        )
                        if pub_date > recent_cutoff:
                            recent_articles.append(url)
                            break
                    except:
                        continue
        
        print(f"Found {len(recent_articles)} recent articles")

async def relevance_scoring():
    async with AsyncUrlSeeder() as seeder:
        # Find pages about Python async programming
        config = SeedingConfig(
            source="sitemap",
            extract_head=True,              # Required for content-based scoring
            query="python async await concurrency",
            scoring_method="bm25",
            score_threshold=0.3,            # Only 30%+ relevant pages
            max_urls=20
        )
        
        urls = await seeder.urls("docs.python.org", config)
        
        # Results are automatically sorted by relevance
        print("Most relevant Python async content:")
        for url in urls[:5]:
            score = url['relevance_score']
            title = url['head_data'].get('title', 'No title')
            print(f"[{score:.2f}] {title}")
            print(f"        {url['url']}")

# URL-based scoring (when extract_head=False)
async def url_based_scoring():
    async with AsyncUrlSeeder() as seeder:
        config = SeedingConfig(
            source="sitemap",
            extract_head=False,             # Fast URL-only scoring
            query="machine learning tutorial",
            scoring_method="bm25",
            score_threshold=0.2
        )
        
        urls = await seeder.urls("example.com", config)
        
        # Scoring based on URL structure, domain, path segments
        for url in urls[:5]:
            print(f"[{url['relevance_score']:.2f}] {url['url']}")

# Multi-concept queries
async def complex_queries():
    queries = [
        "data science pandas numpy visualization",
        "web scraping automation selenium",
        "machine learning tensorflow pytorch",
        "api documentation rest graphql"
    ]
    
    async with AsyncUrlSeeder() as seeder:
        all_results = []
        
        for query in queries:
            config = SeedingConfig(
                source="sitemap",
                extract_head=True,
                query=query,
                scoring_method="bm25",
                score_threshold=0.4,
                max_urls=200
            )
            
            urls = await seeder.urls("realpython.com", config)
            all_results.extend(urls)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_results = []
        for result in all_results:
            if result['url'] not in seen:
                seen.add(result['url'])
                unique_results.append(result)
        
        print(f"Found {len(unique_results)} unique pages across all topics")
        json_str = json.dumps(list(seen), indent=2, ensure_ascii=False)
        print(highlight(json_str, JsonLexer(), Terminal256Formatter()))

### Live URL Validation
async def url_validation(urls):
    seeder = AsyncUrlSeeder()
    config = SeedingConfig(
        # source="sitemap",
        live_check=True,              # Verify URLs are accessible
        concurrency=15,               # Parallel HEAD requests
        hits_per_sec=8,              # Rate limiting
        max_urls=200,
        extract_head=True,            # Get metadata
        verbose=True,
    )
    
    results = await seeder.many_urls(urls, config)
    valid_urls = []
    invalid_urls = []
    for domain, urls in results.items():
        print(f"\nDomain: {domain}")
        print(f"Total URLs checked: {len(urls)}")
        # Analyze results
        valid_urls.extend([u for u in urls if u['status'] == 'valid'])
        invalid_urls.extend([u for u in urls if u['status'] == 'not_valid'])
    
    print(f"✅ Valid URLs: {len(valid_urls)}")
    # print(highlight(json.dumps(valid_urls, indent=2, ensure_ascii=False), JsonLexer(), Terminal256Formatter()))
    print(f"❌ Invalid URLs: {len(invalid_urls)}")
    # print(highlight(json.dumps(invalid_urls, indent=2, ensure_ascii=False), JsonLexer(), Terminal256Formatter()))
    if len(valid_urls) + len(invalid_urls) > 0:
        success_rate = len(valid_urls) / (len(valid_urls) + len(invalid_urls)) * 100
        print(f"📊 Success rate: {success_rate:.1f}%")
    else:
        print("📊 Success rate: N/A (no URLs checked)")
    
    # Show some invalid URLs for debugging
    if invalid_urls:
        print("\nSample invalid URLs:")
        for url in invalid_urls:
            print(f"  - {url['url']}")
