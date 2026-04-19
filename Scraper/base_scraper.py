"""爬虫基类 - 子类只需实现 build_request() 和 parse()"""

import time
import random
import logging
from pathlib import Path
from abc import ABC, abstractmethod
from typing import ClassVar, List, Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class BaseScraper(ABC):
    request_interval: ClassVar[float] = 1.0  # 每次请求后等待的秒数
    max_retries: ClassVar[int] = 3  # 请求失败时的最大重试次数
    request_timeout: ClassVar[int] = 30  # 请求超时时间（秒）
    retry_status_codes: ClassVar[List[int]] = [429, 500, 502, 503, 504]

    # UA池 - 每次请求随机选一个，伪装成真实浏览器
    USER_AGENT_LIST = [
        # Chrome on Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        # Chrome on Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        # Chrome on Linux
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        # Firefox on Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        # Firefox on Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0",
        # Edge on Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
        # Safari on Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    ]

    def __init__(self):
        super().__init__()
        self.session = self._make_session()
        self.last_request_time = (
            time.time() - self.request_interval
        )  # 初始化为足够早的时间，确保第一次请求时不会被延迟

    def _make_session(self):
        s = requests.Session()
        _adapter = self._make_adapter()
        s.mount("http://", _adapter)
        s.mount("https://", _adapter)
        return s

    def _make_adapter(self):
        return HTTPAdapter(
            max_retries=Retry(
                total=self.max_retries,
                backoff_factor=1,
                status_forcelist=self.retry_status_codes,
            )
        )

    def fetch(
        self, url, method="GET", stream_limit_count=0, **kwargs
    ) -> Optional[requests.Response]:
        gap = time.time() - self.last_request_time
        if gap < self.request_interval:
            # 等待足够的时间
            time.sleep(
                self.request_interval - gap + random.uniform(0.1, 0.5)
            )  # 加入随机小延迟，模拟人类行为
        headers = {"User-Agent": random.choice(self.USER_AGENT_LIST)}
        extra_headers = kwargs.pop("headers", {})  # 取出并从kwargs移除
        headers.update(extra_headers)  # 调用方的headers覆盖默认的
        try:
            if method.upper() == "POST":
                response = self.session.post(
                    url, headers=headers, timeout=self.request_timeout, **kwargs
                )
            else:
                response = self.session.get(
                    url, headers=headers, timeout=self.request_timeout, **kwargs
                )
            self.last_request_time = time.time()
            if response.status_code in (403, 429):
                if stream_limit_count < self.max_retries:
                    wait = 30 if response.status_code == 403 else 45
                    print(f"被限流({response.status_code})，等待{wait}s后重试...")
                    return self.fetch(
                        url, method, stream_limit_count=stream_limit_count + 1, **kwargs
                    )
                else:
                    print(
                        f"已重试{self.max_retries}次，仍被限流({response.status_code})，放弃请求: {url}"
                    )
                    return None
            return response

        except requests.RequestException as e:
            self.last_request_time = time.time()
            print(f"请求失败: {e} (URL: {url})")
            return None
        
    @abstractmethod
    def build_request(self, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def parse(self, response: requests.Response) -> Any:
        raise NotImplementedError
    
    def run(self, **kwargs):
        req = self.build_request(**kwargs)
        response = self.fetch(**req)
        if response is None:
            return None
        return self.parse(response)
    