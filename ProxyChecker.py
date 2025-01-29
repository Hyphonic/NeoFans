import asyncio
import httpx
import re
import os

class ProxyChecker:
    def __init__(self, max_workers: int = 100):
        self.Semaphore = asyncio.Semaphore(max_workers)
        self.WorkingProxies = []
        self.ProxyPattern = re.compile(r'(?:socks5?://)?(?:http://)?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+)')

    async def FetchProxies(self, url: str) -> list[str]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return [proxy.strip() for proxy in response.text.strip().split('\n') if proxy.strip()]
            except Exception as e:
                print(f'Failed to fetch proxies: {e}')
        return []

    async def CheckProxy(self, proxy: str):
        match = self.ProxyPattern.search(proxy)
        clean_proxy = match.group(1) if match else proxy
        formatted_proxy = f'socks5://{clean_proxy}'

        async with self.Semaphore:
            try:
                async with httpx.AsyncClient(
                    proxy=formatted_proxy,
                    timeout=10.0,
                    follow_redirects=True
                ) as client:
                    response = await client.get('https://google.com')
                    if response.status_code == 200:
                        self.WorkingProxies.append(formatted_proxy)
                        print(f'Working proxy: {formatted_proxy}')
            except Exception:
                pass

    async def SaveProxies(self):
        if self.WorkingProxies:
            os.makedirs('Proxies', exist_ok=True)
            with open('Proxies/Socks5.txt', 'w') as f:
                f.write('\n'.join(self.WorkingProxies))
            print(f'Saved {len(self.WorkingProxies)} working proxies')

async def Main():
    checker = ProxyChecker()
    proxies = await checker.FetchProxies(
        'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/socks5.txt'
    )
    
    await asyncio.gather(*(checker.CheckProxy(proxy) for proxy in proxies))
    await checker.SaveProxies()

if __name__ == '__main__':
    try:
        asyncio.run(Main())
    except KeyboardInterrupt:
        print('Program interrupted by user')
    except Exception as e:
        print(f'Error: {e}')
    finally:
        print('Exiting program')
