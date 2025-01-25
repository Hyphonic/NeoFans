from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict
from rich.console import Console
import requests
import time
import os

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

class ProxyChecker:
    def __init__(self, ProxyUrls: Dict[str, List[str]], Timeout: int = 1, MaxRetries: int = 3, RetryDelay: float = 1.0, MaxWorkers: int = 20):
        self.ProxyUrls = ProxyUrls  # Dict[str, List[str]]
        self.Timeout = Timeout
        self.MaxRetries = MaxRetries
        self.RetryDelay = RetryDelay
        self.MaxWorkers = MaxWorkers
        self.TotalProxiesChecked = 0
        self.WorkingProxiesFound = 0
        self.Session = requests.Session()

    def CheckProxy(self, Proxy: str) -> Optional[str]:
        try:
            Session = requests.Session()
            Session.proxies = {'http': Proxy, 'https': Proxy}
            Response = Session.get('http://www.google.com', timeout=self.Timeout)
            if Response.status_code == 200:
                return Proxy
        except requests.RequestException:
            return None

    def GetProxies(self, Url: str) -> List[str]:
        for Attempt in range(self.MaxRetries):
            try:
                Response = self.Session.get(Url, timeout=self.Timeout)
                Response.raise_for_status()
                #Logger.info(f"∙ Successfully fetched proxies from {Url}")
                return Response.text.strip().splitlines()
            except requests.RequestException as E:
                Logger.warning(f"∙ Attempt {Attempt + 1} failed to retrieve proxies from {Url}: {E}")
                time.sleep(self.RetryDelay)
        Logger.error(f"Failed to retrieve proxies from {Url} after {self.MaxRetries} attempts")
        return []

    @staticmethod
    def CreateProxyDir(Directory: str) -> None:
        os.makedirs(Directory, exist_ok=True)

    def ProcessProxies(self, ProxyType: str, Url: str) -> None:
        ProxyDir = f'proxies/{ProxyType}.txt'
        self.CreateProxyDir(os.path.dirname(ProxyDir))
        Proxies = self.GetProxies(Url)
        TotalProxies = len(Proxies)
        
        if not Proxies:
            Logger.warning(f"No proxies to check for {ProxyType} from {Url}")
            return
        
        Logger.info(f"Checking {TotalProxies} {ProxyType} proxies from {Url} using {self.MaxWorkers} workers")

        WorkingProxies = []
        with ThreadPoolExecutor(max_workers=self.MaxWorkers) as Executor:
            Futures = {Executor.submit(self.CheckProxy, Proxy): Proxy for Proxy in Proxies}
            for Future in as_completed(Futures):
                Result = Future.result()
                if Result:
                    WorkingProxies.append(Result)

        try:
            with open(ProxyDir, 'a') as File:  # Append to avoid overwriting
                File.write('\n'.join(WorkingProxies) + '\n')
        except OSError as E:
            Logger.error(f"Failed to write working proxies to {ProxyDir}: {E}")

        Logger.info(f"∙ Found {len(WorkingProxies)} working {ProxyType} proxies out of {TotalProxies} from {Url}")
        self.TotalProxiesChecked += TotalProxies
        self.WorkingProxiesFound += len(WorkingProxies)

    def Run(self) -> None:
        StartTime = time.time()
        
        try:
            for ProxyType, Urls in self.ProxyUrls.items():
                for Url in Urls:
                    self.ProcessProxies(ProxyType, Url)
        except KeyboardInterrupt:
            Logger.warning("Process interrupted by user")
        finally:
            self.Session.close()

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
        "socks4": [
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
            "https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/socks4/data.txt"
        ],
        "socks5": [
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
            "https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/socks5/data.txt"
        ]
    }
    Console(force_terminal=True).print(Screen)
    Checker = ProxyChecker(ProxyUrls, MaxWorkers=150)
    Checker.Run()