from typing import List, Optional, Dict
from rich.console import Console
import httpx
import time
import os
import re
import asyncio
from rich.progress import Progress, BarColumn, TimeElapsedColumn

LogLevel = 0  # 0: Debug, 1: Info, 2: Warning, 3: Error, 4: Critical

class RichLogger:
    def __init__(self, name=__name__):
        self.Console = Console(
            markup=True,
            log_time=False,
            force_terminal=True,
            width=140
        )
    
    LogLevels = {
        'DEBUG': 1,
        'INFO': 2,
        'WARNING': 3,
        'ERROR': 4,
        'CRITICAL': 5
    }

    def debug(self, message: str):
        if LogLevel <= self.LogLevels['DEBUG']:
            self.Console.log(f"[bold blue]DEBUG:   [/bold blue] {message}")

    def info(self, message: str):
        if LogLevel <= self.LogLevels['INFO']:
            self.Console.log(f"[bold green]INFO:    [/bold green] {message}")

    def warning(self, message: str):
        if LogLevel <= self.LogLevels['WARNING']:
            self.Console.log(f"[bold yellow]WARNING: [/bold yellow] {message}")

    def error(self, message: str):
        if LogLevel <= self.LogLevels['ERROR']:
            self.Console.log(f"[bold red]ERROR:   [/bold red] {message}")

Logger = RichLogger(__name__)

Screen = r'''
 ______   ______     ______     __  __     __  __    
/\  == \ /\  == \   /\  __ \   /\_\_\_\   /\ \_\ \   
\ \  _-/ \ \  __<   \ \ \/\ \  \/_/\_\/_  \ \____ \  
 \ \_\    \ \_\ \_\  \ \_____\   /\_\/\_\  \/\_____\ 
  \/_/     \/_/ /_/   \/_____/   \/_/\/_/   \/_____/ 
                                                     
'''

TEST_SITES = [
    'http://www.google.com',
    'http://www.amazon.com',
    'http://www.github.com',
    'http://www.cloudflare.com',
    'http://www.reddit.com'
]

class ProxyChecker:
    def __init__(self, max_workers: int = 100):
        self.Semaphore = asyncio.Semaphore(max_workers)
        self.WorkingProxies: Dict[str, List[str]] = {
            'http': [],
            'socks5': []
        }
        self.TotalChecked = 0
        self.TotalWorking = 0
        self.ProxyPattern = re.compile(r'(?:https?://)?(?:http://)?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+)')
        self.Lock = asyncio.Lock()  # Add lock for thread-safe updates

    async def FetchProxies(self, urls: List[str]) -> List[str]:
        AllProxies = []
        async with httpx.AsyncClient(timeout=10.0) as Client:
            for Url in urls:
                try:
                    Response = await Client.get(Url)
                    if Response.status_code == 200:
                        Proxies = Response.text.strip().split('\n')
                        AllProxies.extend([Proxy.strip() for Proxy in Proxies if Proxy.strip()])
                except Exception as e:
                    Logger.error(f"Failed to fetch proxies from {Url}: {e}")
        return list(set(AllProxies))

    async def CheckProxy(self, Proxy: str, ProxyType: str, ProgressBar, Task, TotalProxies: int):
        TestSite = TEST_SITES[int(time.time()) % len(TEST_SITES)]
        
        # Extract clean IP:PORT from proxy string
        Match = self.ProxyPattern.search(Proxy)
        CleanProxy = Match.group(1) if Match else Proxy
        FormattedProxy = f"{ProxyType}://{CleanProxy}"
        
        async with self.Semaphore:
            try:
                async with httpx.AsyncClient(
                    proxy=FormattedProxy,
                    timeout=10.0,
                    follow_redirects=True
                ) as Client:
                    StartTime = time.time()
                    Response = await Client.get(TestSite)
                    Latency = int((time.time() - StartTime) * 1000)  # Convert to ms
                    
                    async with self.Lock:  # Use lock for thread-safe updates
                        self.TotalChecked += 1
                        if Response.status_code == 200:
                            self.WorkingProxies[ProxyType].append(FormattedProxy)
                            self.TotalWorking += 1
                            
                        ProgressBar.update(
                            Task,
                            description=f"[blue]{ProxyType.upper()}[/blue]",
                            advance=1,
                            proxy=FormattedProxy,
                            stats=f"{self.TotalWorking}/{TotalProxies}",  # Use fixed total
                            latency=f"{Latency}ms",
                            status="[green]Working[/green]" if Response.status_code == 200 
                                else f"[red]Bad Status: {Response.status_code}[/red]"
                        )
                        
            except Exception as e:
                async with self.Lock:
                    self.TotalChecked += 1
                    ProgressBar.update(
                        Task,
                        description=f"[blue]{ProxyType.upper()}[/blue]",
                        advance=1,
                        proxy=FormattedProxy,
                        stats=f"{self.TotalWorking}/{TotalProxies}",  # Use fixed total
                        latency="N/A",
                        status="[red]Failed[/red]"
                    )
            
            ProgressBar.refresh()

    async def SaveProxies(self):
        os.makedirs('proxies', exist_ok=True)
        for ProxyType, Proxies in self.WorkingProxies.items():
            if Proxies:
                async with aiofiles.open(f'proxies/{ProxyType}.txt', 'w') as f:
                    await f.write('\n'.join(Proxies))
                Logger.info(f"Saved {len(Proxies)} {ProxyType} proxies")

async def Main():
    Console(force_terminal=True).print(Screen)
    Checker = ProxyChecker()
    
    with Progress(
        "[progress.description]{task.description}",
        BarColumn(bar_width=100),  # Set fixed width to 50 characters
        "[progress.percentage]{task.percentage:>3.0f}%",
        "•",
        "{task.fields[proxy]}",
        "•",
        "{task.fields[stats]}",
        "•",
        "{task.fields[latency]}",
        "•",
        "{task.fields[status]}",
        TimeElapsedColumn(),
        console=Console(force_terminal=True),
        auto_refresh=False,
        expand=True
    ) as ProgressBar:
        for ProxyType, Urls in ProxyUrls.items():
            Logger.info(f"Fetching {ProxyType} proxies...")
            RawProxies = await Checker.FetchProxies(Urls)
            TotalProxies = len(RawProxies)  # Get total once
            Logger.info(f"Found {TotalProxies} {ProxyType} proxies to check")
            
            Task = ProgressBar.add_task(
                "",
                total=TotalProxies,
                proxy="",
                stats=f"0/{TotalProxies}",  # Initialize with correct total
                latency="0ms",
                status=""
            )
            
            Tasks = []
            for Proxy in RawProxies:
                if ProxyType == 'http' and 'https://' in Proxy:
                    Proxy = Proxy.replace('https://', 'http://')
                Tasks.append(Checker.CheckProxy(
                    Proxy, 
                    ProxyType, 
                    ProgressBar, 
                    Task,
                    TotalProxies  # Pass fixed total to CheckProxy
                ))
            
            Logger.info(f"Checking {ProxyType} proxies...")
            await asyncio.gather(*Tasks)

    await Checker.SaveProxies()
    Logger.info("Proxy check completed!")
    for ProxyType, Proxies in Checker.WorkingProxies.items():
        Logger.info(f"Working {ProxyType} proxies: {len(Proxies)}")

if __name__ == "__main__":
    ProxyUrls = {
        "http": [
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/http/data.txt"
        ],
        "socks5": [
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
            "https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/socks5/data.txt"
        ]
    }
    import aiofiles
    try:
        asyncio.run(Main())
    except KeyboardInterrupt:
        Logger.warning("Program interrupted by user")
    except Exception as e:
        Console(force_terminal=True).print_exception()
    finally:
        Logger.info("Exiting program")