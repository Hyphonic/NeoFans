from rich.console import Console
from proxybroker.api import Broker as BaseBroker
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

class ModernBroker(BaseBroker):
    def __init__(self, queue=None):
        super().__init__(queue)
        self._on_check = asyncio.Queue(maxsize=self._max_conn)

async def Show(Proxies, Progress, Task):
    count = 0
    while True:
        try:
            Proxy = await Proxies.get()
            if Proxy is None:
                break
            
            count += 1
            Progress.update(Task, advance=1)
            
            os.makedirs('proxies', exist_ok=True)
            async with aiofiles.open(f'proxies/socks5.txt', 'a') as f:
                await f.write(f'socks5://{Proxy.host}:{Proxy.port}\n')
            
        except Exception as e:
            Logger.Error(f'Error processing proxy: {e}')

async def Main():
    Logger.Info('Starting Proxy Checker')
    Proxies = asyncio.Queue()
    BrokerClient = ModernBroker(Proxies)
    ProxyLimit = 100

    with Progress(
        '[progress.description]{task.description}',
        BarColumn(),
        '[progress.percentage]{task.percentage:>3.0f}%',
        TimeElapsedColumn(),
        console=Console(force_terminal=True),
        auto_refresh=True
    ) as Progress:
        Task = Progress.add_task(
            '[blue]Finding Proxies[/blue]',
            total=ProxyLimit
        )
        
        await asyncio.gather(
            BrokerClient.find(types=['SOCKS5'], limit=ProxyLimit),
            Show(Proxies, Progress, Task)
        )

if __name__ == '__main__':
    asyncio.run(Main())