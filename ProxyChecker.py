from rich.console import Console
from proxybroker import Broker
import aiofiles
import asyncio
import os

from rich.progress import Progress
from rich.progress import (
    BarColumn,
    TimeElapsedColumn
)

LOG_LEVEL = 0  # 0: Debug, 1: Info, 2: Warning, 3: Error, 4: Critical

class RichLogger:
    def __init__(self):
        self.Console = Console(
            markup=True,
            log_time=False,
            force_terminal=True,
            width=140,
            log_path=False
        )
    
    LogLevels = {
        'DEBUG': 1,
        'INFO': 2,
        'WARNING': 3,
        'ERROR': 4,
        'CRITICAL': 5
    }

    def Debug(self, Message):
        if LOG_LEVEL <= self.LogLevels['DEBUG']:
            self.Console.log(f'[bold blue]DEBUG:   [/bold blue] {Message}')

    def Info(self, Message):
        if LOG_LEVEL <= self.LogLevels['INFO']:
            self.Console.log(f'[bold green]INFO:    [/bold green] {Message}')

    def Warning(self, Message):
        if LOG_LEVEL <= self.LogLevels['WARNING']:
            self.Console.log(f'[bold yellow]WARNING: [/bold yellow] {Message}')

    def Error(self, Message):
        if LOG_LEVEL <= self.LogLevels['ERROR']:
            self.Console.log(f'[bold red]ERROR:   [/bold red] {Message}')

    def Critical(self, Message):
        if LOG_LEVEL <= self.LogLevels['CRITICAL']:
            self.Console.log(f'[bold magenta]CRITICAL:[/bold magenta] {Message}')

Logger = RichLogger()

async def Show(Proxies):
    Count = 0
    while True:
        try:
            Proxy = await Proxies.get()
            if Proxy is None:
                Logger.Info('Proxy Queue is Empty')
                break
            
            Count += 1
            Logger.Info(f'Found Proxy: {Proxy.types.pop().lower()}://{Proxy.host}:{Proxy.port}')
            os.makedirs('proxies', exist_ok=True)
            
            async with aiofiles.open(f'proxies/{Proxy.types.pop().lower()}.txt', 'a') as File:
                await File.write(f'{Proxy.types.pop().lower()}://{Proxy.host}:{Proxy.port}\n')
            
        except Exception as e:
            Logger.Error(f'Error processing proxy: {e}')

async def Main():
    Proxies = asyncio.Queue()
    BrokerClient = Broker(Proxies)
    ProxyLimit = 100

    Logger.Info(f'Scraping {ProxyLimit} Proxies')
    
    Tasks = asyncio.gather(
        BrokerClient.find(types=['SOCKS5'], limit=ProxyLimit),
        Show(Proxies)
    )
    await Tasks

if __name__ == '__main__':
    asyncio.run(Main())