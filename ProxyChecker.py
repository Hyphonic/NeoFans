from typing import List, Optional, Dict
from rich.console import Console
import httpx
import time
import os
import aiofiles
import asyncio
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
)

LogLevel = 4  # Disable all logging except for the summary at the end

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

class ProxyChecker:
    def __init__(self, ProxyUrls: Dict[str, List[str]], Timeout: int = 1, MaxRetries: int = 3, RetryDelay: float = 1.0, MaxWorkers: int = 20):
        self.ProxyUrls = ProxyUrls  # Dict[str, List[str]]
        self.Timeout = Timeout
        self.MaxRetries = MaxRetries
        self.RetryDelay = RetryDelay
        self.MaxWorkers = min(MaxWorkers, 50)  # Limit the number of workers to reduce memory usage
        self.TotalProxiesChecked = 0
        self.WorkingProxiesFound = 0
        self.Client = httpx.AsyncClient()
        self.Semaphore = asyncio.Semaphore(self.MaxWorkers)
        self.TestUrls = [
            'http://www.google.com',
        ]

    async def CheckProxy(self, Proxy: str, ProxyType: str) -> Optional[str]:
        async with self.Semaphore:  # Limit concurrent connections
            for TestUrl in self.TestUrls:
                try:
                    if ProxyType in ["http", "https"]:
                        ProxyUrl = f"http://{Proxy}"
                    else:
                        ProxyUrl = f"{ProxyType}://{Proxy}"

                    async with httpx.AsyncClient(
                        proxy=ProxyUrl,
                        timeout=self.Timeout,
                        follow_redirects=True
                    ) as Client:
                        Start = time.time()
                        Response = await Client.get(TestUrl)
                        End = time.time()
                        
                        if Response.status_code == 200 and (End - Start) < self.Timeout:
                            return Proxy
                except Exception:
                    continue
            return None

    async def GetProxies(self, Url: str) -> List[str]:
        for Attempt in range(self.MaxRetries):
            try:
                Response = await self.Client.get(Url)
                Response.raise_for_status()
                return Response.text.strip().splitlines()
            except httpx.RequestError as E:
                Logger.warning(f"∙ Attempt {Attempt + 1} failed to retrieve proxies from {Url}: {E}")
                await asyncio.sleep(self.RetryDelay)
        Logger.error(f"Failed to retrieve proxies from {Url} after {self.MaxRetries} attempts")
        return []

    @staticmethod
    def CreateProxyDir(Directory: str) -> None:
        os.makedirs(Directory, exist_ok=True)

    async def ProcessProxies(self, ProxyType: str, Url: str) -> None:
        ProxyDir = f'proxies/{ProxyType}.txt'
        self.CreateProxyDir(os.path.dirname(ProxyDir))
        
        try:
            Proxies = await self.GetProxies(Url)
            if not Proxies:
                return

            WorkingProxies = []
            BatchSize = 50  # Process proxies in batches
            
            # Create progress bar
            ProgressColumns = [
                TextColumn("{task.fields[proxy_type]}"),
                BarColumn(bar_width=None),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("•"),
                TextColumn("[blue]{task.fields[proxy]}"),
                TextColumn("•"),
                MofNCompleteColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
            ]

            with Progress(*ProgressColumns, console=Logger.Console, auto_refresh=False, expand=True) as progress:
                task_id = progress.add_task(
                    "Checking proxies",
                    total=len(Proxies),
                    proxy_type=ProxyType.capitalize(),
                    proxy="Starting..."
                )

                # Process in batches
                for i in range(0, len(Proxies), BatchSize):
                    Batch = Proxies[i:i + BatchSize]
                    Tasks = [self.CheckProxy(Proxy, ProxyType) for Proxy in Batch]
                    
                    Results = await asyncio.gather(*Tasks, return_exceptions=True)
                    
                    for Result in Results:
                        if isinstance(Result, str):  # Valid proxy
                            WorkingProxies.append(Result)
                        progress.update(task_id, advance=1, proxy=Result if isinstance(Result, str) else "Failed")
                        progress.refresh()

            # Save working proxies
            if WorkingProxies:
                async with aiofiles.open(ProxyDir, 'a') as File:
                    await File.write('\n'.join(WorkingProxies) + '\n')

            self.TotalProxiesChecked += len(Proxies)
            self.WorkingProxiesFound += len(WorkingProxies)
            
        except Exception as E:
            Logger.error(f"Error processing {ProxyType} proxies: {str(E)}")

    async def Run(self) -> None:
        StartTime = time.time()
        
        try:
            Tasks = []
            for ProxyType, Urls in self.ProxyUrls.items():
                for Url in Urls:
                    Tasks.append(self.ProcessProxies(ProxyType, Url))
            await asyncio.gather(*Tasks)
        except KeyboardInterrupt:
            Logger.warning("Process interrupted by user")
        finally:
            await self.Client.aclose()

        EndTime = time.time()
        ExecutionTime = EndTime - StartTime
        Minutes, Seconds = divmod(ExecutionTime, 60)
        Logger.info(f"Total proxies checked: {self.TotalProxiesChecked}")
        Logger.info(f"Working proxies found: {self.WorkingProxiesFound}")
        Logger.info(f"Execution time: {int(Minutes)}m {int(Seconds)}s")

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
    Console(force_terminal=True).print(Screen)
    Checker = ProxyChecker(ProxyUrls, MaxWorkers=100)  # Limit the number of workers to reduce memory usage
    asyncio.run(Checker.Run())

        # "socks4": [
        #     "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        #     "https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/socks4/data.txt"
        # ],