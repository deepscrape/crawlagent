import logging
import re
from typing import cast

from crawl4ai import (
    AsyncWebCrawler,
    CacheMode,
    CrawlerMonitor,
    CrawlResult,
    ExtractionStrategy,
    LLMConfig,
    LLMExtractionStrategy,
    LXMLWebScrapingStrategy,
    MemoryAdaptiveDispatcher,
    RateLimiter,
)
from crawl4ai.async_configs import CrawlerRunConfig
from pydantic import BaseModel, Field

logger = logging.getLogger("crawlagent")

async def infinite_scroll(
    crawler: AsyncWebCrawler,
    markdown_generator,
    url="https://www.scrapingcourse.com/infinite-scrolling",
    session_id="hn_session",
):
    # Wait until new commits appear
    wait_condition = """js:() => {
            const items = document.querySelectorAll('same4all');
            return items?.length >= 0;
        }"""

    # Step 1: Load initial commits
    next_config = CrawlerRunConfig(
        wait_for=wait_condition,
        session_id=session_id,
        cache_mode=CacheMode.BYPASS,
        markdown_generator=markdown_generator,
    )

    result = cast(CrawlResult, await crawler.arun(
        url=url,  # "https://lnk.bio/akis_petretzikis"
        config=next_config,
    ))

    # selectors = auto_detect_selectors_with_dynamic_attributes(result.html)
    # print("CSS Selectors:", selectors['css_selectors'])

    selector = "div.pb-linkbox-inner.lnkbio-boxcolor.rounded.p-2.d-flex.flex-column.align-items-center.justify-content-center.ms-0.me-0"
    pages_to_scroll = 3

    items = result.cleaned_html.count("<img") * pages_to_scroll

    print("Initial products loaded. Count:", result.cleaned_html.count("<img"), items)

    # Wait until new commits appear
    wait_for_more = f"""js:() => {{
            const items = document.querySelectorAll('{selector}');
            return items?.length > {items};
        }}"""

    next_config1 = CrawlerRunConfig(
        session_id=session_id,
        wait_for=wait_for_more,
        wait_for_images=True,  # Add this argument to ensure images are fully loaded
        js_only=True,  # We're continuing from the open tab
        cache_mode=CacheMode.BYPASS,
        scan_full_page=True,  # Enables scrolling
        markdown_generator=markdown_generator,
    )

    # use second time
    result2 = cast(CrawlResult, await crawler.arun(url=url, config=next_config1))

    # Step 3: Extract data from the page
    # Access different media types
    # images = result2.media["images"]  # List of image details
    # videos = result2.media["videos"]  # List of video details
    # audios = result2.media["audios"]  # List of audio details
    # print("video: ", videos, "images: ", images, "audios: ", audios)

    # # Image with metadata
    # for image in images:
    #     print(f"URL: {image['src']}")
    #     print(f"Alt text: {image['alt']}")
    #     print(f"Description: {image['desc']}")
    #     print(f"Relevance score: {image['score']}")

    # # Process videos
    # for video in videos:
    #     print(f"Video source: {video['src']}")
    #     print(f"Type: {video['type']}")
    #     print(f"Duration: {video.get('duration')}")
    #     print(f"Thumbnail: {video.get('poster')}")

    # # Process audio
    # for audio in audios:
    #     print(f"Audio source: {audio['src']}")
    #     print(f"Type: {audio['type']}")
    #     print(f"Duration: {audio.get('duration')}")

    # Process content after each load
    # print(f" items:",result2.metadata, result2.cleaned_html)

    # print()  # Print clean markdown content
    # selectors = auto_detect_selectors_with_dynamic_attributes(result.html)
    # print("CSS Selectors:", selectors['css_selectors'])
    # print("XPath Selectors:", selectors['xpath_selectors'])

    # print("After scroll+click, length:", len(result2.html))
    # total_items = result2.html
    return result2


async def load_more(
    crawler: AsyncWebCrawler,
    markdown_generator,
    proxy_rotation_strategy,
    url="https://www.scrapingcourse.com/button-click",
    session_id="hn_session",
):
    # Wait until new commits appea
    wait_condition = """js:() => {
        const items = document.querySelectorAll('.product-item');
        return items.length > 0;
    }"""

    # Step 1: Load initial commits
    next_config = CrawlerRunConfig(
        scraping_strategy=LXMLWebScrapingStrategy(),
        wait_for=wait_condition,
        session_id=session_id,
        cache_mode=CacheMode.BYPASS,
        markdown_generator=markdown_generator,
        proxy_rotation_strategy=proxy_rotation_strategy,
        # Not using js_only yet since it's our first load
    )

    # run_config = CrawlerRunConfig(
    #     # wait_for=f"css:{bg_images}",
    #     session_id=session_id,
    #     wait_for=f"js:{wait_condition}",
    #     # wait_for_images=True,  # Add this argument to ensure images are fully loaded
    #     # process_iframes=True,  # Process iframes
    #     # remove_overlay_elements=True,  # Remove popups/modals that might block iframe
    #     # js_code=js_commands, # where js_code is the previous code content
    #     # magic=True,  # Enable magic mode
    #     scan_full_page=True,   # Enables scrolling
    #     scroll_delay=2, # Waits 200ms between scrolls (optional)
    #     # Mark that we do not re-navigate, but run JS in the same session:
    #     js_only=True,
    #     # cache_mode=CacheMode.BYPASS,  # New way to handle cache
    #     # Only execute JS without reloading page
    #     # simulate_user=True,      # Simulate human behavior
    #     # override_navigator=True,  # Override navigator properties
    # )   # Default crawl run configuration

    result = cast (CrawlResult, await crawler.arun(url=url, config=next_config))

    selector = "button#load-more-btn"
    pages_to_load = 3
    items = result.cleaned_html.count("product-image") if result.cleaned_html else 0
    # print("Initial products loaded. Count:", result.cleaned_html.count("product-image") if result.cleaned_html else 0)

    # Step 2: For subsequent pages, we run JS to click 'Next Page' if it exists
    js_next_page = f""" 
        window.scrollTo(0, document.body.scrollHeight);
        const selector = '{selector}';
        const button = document.querySelector(selector);
        if(button) button.click();
    """

    # Wait until new commits appear
    wait_for_more = f"""js:() => {{
        const items = document.querySelectorAll('.product-item');
        return items.length > {items * pages_to_load};
    }}"""

    for page in range(1):  # let's do 2 more "Next" pages
        config_next = CrawlerRunConfig(
            scraping_strategy=LXMLWebScrapingStrategy(),
            session_id=session_id,
            js_code=[
                js_next_page,
                js_next_page,
                js_next_page,
            ],  # Execute JS to click 'Load more'
            wait_for=wait_for_more,
            js_only=True,  # We're continuing from the open tab
            cache_mode=CacheMode.BYPASS,
            markdown_generator=markdown_generator,
            proxy_rotation_strategy=proxy_rotation_strategy,
            # magic=True,  # Enable magic mode
            # scan_full_page=True,   # Enables scrolling
            # scroll_delay=2, # Waits 200ms between scrolls (optional)
        )
        result2 = cast(CrawlResult, await crawler.arun(url=url, config=config_next))
        logger.info(f"Page {page + 2} items count:")
        # # Step 3: Extract data from the page
        # # Access different media types
        # images = result2.media["images"]  # List of image details
        # videos = result2.media["videos"]  # List of video details
        # audios = result2.media["audios"]  # List of audio details
        # # print("video: ", videos, "images: ", images, "audios: ", audios)

        # # # Image with metadata
        # for image in images:
        #     print(f"URL: {image['src']}")
        #     print(f"Alt text: {image['alt']}")
        #     print(f"Description: {image['desc']}")
        #     print(f"Relevance score: {image['score']}")

        # # Process videos
        # for video in videos:
        #     print(f"Video source: {video['src']}")
        #     print(f"Type: {video['type']}")
        #     print(f"Duration: {video.get('duration')}")
        #     print(f"Thumbnail: {video.get('poster')}")

        # # Process audio
        # for audio in audios:
        #     print(f"Audio source: {audio['src']}")
        #     print(f"Type: {audio['type']}")
        #     print(f"Duration: {audio.get('duration')}")

        # print()  # Print clean markdown content
        # selectors = auto_detect_selectors_with_dynamic_attributes(result.html)
        # print("CSS Selectors:", selectors['css_selectors'])
        # print("XPath Selectors:", selectors['xpath_selectors'])

        # print("After scroll+click, length:", len(result2.html))
        # total_items = result2.html
        ip_match = re.search(r'(?:[0-9]{1,3}\.){3}[0-9]{1,3}', result.html)
        current_proxy = config_next.proxy_config if config_next.proxy_config else None

        if current_proxy and ip_match:
            print(f"URL {result.url}")
            print(f"Proxy {current_proxy.server} -> Response IP: {ip_match.group(0)}")
            verified = ip_match.group(0) == current_proxy.ip
            if verified:
                print(f"✅ Proxy working! IP matches: {current_proxy.ip}")
            else:
                print("❌ Proxy failed or IP mismatch!")
        print("---")
        return result2


async def basic_crawl(
    crawler: AsyncWebCrawler,
    markdown_generator,
    proxy_rotation_strategy = None,
    url="https://www.scrapingcourse.com/button-click",
    session_id="hn_session",
    llm_strategy = None,
    stream: bool = False,
):
    # wait_condition = """js:() => {
    #     const isButton = document.querySelector('button.fc-button.fc-confirm-choices.fc-primary-button');
    #     isButton?.click();
    #     return isButton !== null;
    # }"""

    # Step 1: Load initial commits
    next_config = CrawlerRunConfig(
        # scraping_strategy=LXMLWebScrapingStrategy(),
        stream=stream,  # Enable streaming
        # wait_for=wait_condition,
        session_id=session_id,
        cache_mode=CacheMode.BYPASS,
        # magic=True,  # Enable magic mode
        markdown_generator=markdown_generator,
        # wait_for_images=True,  # Add this argument to ensure images are fully loaded
        # process_iframes=True, # Extract iframe content
        # remove_overlay_elements=True,  # Remove popups/modals that might block iframe
        # simulate_user=True, # Simulate human behavior
        # override_navigator=True,  # Override navigator properties
        # proxy_rotation_strategy=proxy_rotation_strategy,
        exclude_external_links=True,
        exclude_social_media_links=True,
        extraction_strategy=llm_strategy
    )

    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=70.0,
        check_interval=1.0,
        max_session_permit=10,  # Maximum concurrent tasks
        rate_limiter=RateLimiter(base_delay=(0.5, 1.0), max_delay=10.0),
        monitor=CrawlerMonitor(enable_ui=True),
    )

    result2 = cast(CrawlResult, await crawler.arun(url=url, config=next_config))

    # config_next1 = CrawlerRunConfig(
    #     session_id=session_id,
    #     js_only=True,       # We're continuing from the open tab
    #     cache_mode=CacheMode.BYPASS,
    #     markdown_generator=markdown_generator,    #
    #     # scan_full_page=True,   # Enables scrolling
    #     # scroll_delay=2, # Waits 200ms between scrolls (optional)
    # )

    # await crawler.arun(
    #     url=url,
    #     config=config_next1
    # )
    # run_config = CrawlerRunConfig(
    #     # wait_for=f"css:{bg_images}",
    #     session_id=session_id,
    #     wait_for=f"js:{wait_condition}",
    #     # wait_for_images=True,  # Add this argument to ensure images are fully loaded
    #     # process_iframes=True,  # Process iframes
    #     # remove_overlay_elements=True,  # Remove popups/modals that might block iframe
    #     # js_code=js_commands, # where js_code is the previous code content
    #     # magic=True,  # Enable magic mode
    #     scan_full_page=True,   # Enables scrolling
    #     scroll_delay=2, # Waits 200ms between scrolls (optional)
    #     # Mark that we do not re-navigate, but run JS in the same session:
    #     js_only=True,
    #     # cache_mode=CacheMode.BYPASS,  # New way to handle cache
    #     # Only execute JS without reloading page
    #     # simulate_user=True,      # Simulate human behavior
    #     # override_navigator=True,  # Override navigator properties
    # )   # Default crawl run configuration

    return result2

async def basic_stream_crawl(
    crawler: AsyncWebCrawler,
    markdown_generator,
    url="https://www.scrapingcourse.com/button-click",
    session_id="hn_session",
    llm_strategy = None,
    stream: bool = False,
):
    # wait_condition = """js:() => {
    #     const isButton = document.querySelector('button.fc-button.fc-confirm-choices.fc-primary-button');
    #     isButton?.click();
    #     return isButton !== null;
    # }"""

    # Step 1: Load initial commits
    next_config = CrawlerRunConfig(
        scraping_strategy=LXMLWebScrapingStrategy(),
        stream=stream,  # Enable streaming
        # wait_for=wait_condition,
        session_id=session_id,
        cache_mode=CacheMode.BYPASS,
        magic=True,  # Enable magic mode
        markdown_generator=markdown_generator,
        # wait_for_images=True,  # Add this argument to ensure images are fully loaded
        # process_iframes=True, # Extract iframe content
        # remove_overlay_elements=True,  # Remove popups/modals that might block iframe
        # simulate_user=True, # Simulate human behavior
        # override_navigator=True,  # Override navigator properties
        exclude_external_links=True,
        exclude_social_media_links=True,
        extraction_strategy=llm_strategy
    )

    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=70.0,
        check_interval=1.0,
        max_session_permit=10,  # Maximum concurrent tasks
        rate_limiter=RateLimiter(base_delay=(0.5, 1.0), max_delay=10.0),
        monitor=CrawlerMonitor(enable_ui=True),
    )

    result2 = await crawler.arun(url=url, config=next_config)
    
    # config_next1 = CrawlerRunConfig(
    #     session_id=session_id,
    #     js_only=True,       # We're continuing from the open tab
    #     cache_mode=CacheMode.BYPASS,
    #     markdown_generator=markdown_generator,    #
    #     # scan_full_page=True,   # Enables scrolling
    #     # scroll_delay=2, # Waits 200ms between scrolls (optional)
    # )

    # await crawler.arun(
    #     url=url,
    #     config=config_next1
    # )
    # run_config = CrawlerRunConfig(
    #     # wait_for=f"css:{bg_images}",
    #     session_id=session_id,
    #     wait_for=f"js:{wait_condition}",
    #     # wait_for_images=True,  # Add this argument to ensure images are fully loaded
    #     # process_iframes=True,  # Process iframes
    #     # remove_overlay_elements=True,  # Remove popups/modals that might block iframe
    #     # js_code=js_commands, # where js_code is the previous code content
    #     # magic=True,  # Enable magic mode
    #     scan_full_page=True,   # Enables scrolling
    #     scroll_delay=2, # Waits 200ms between scrolls (optional)
    #     # Mark that we do not re-navigate, but run JS in the same session:
    #     js_only=True,
    #     # cache_mode=CacheMode.BYPASS,  # New way to handle cache
    #     # Only execute JS without reloading page
    #     # simulate_user=True,      # Simulate human behavior
    #     # override_navigator=True,  # Override navigator properties
    # )   # Default crawl run configuration

    return result2.extracted_content