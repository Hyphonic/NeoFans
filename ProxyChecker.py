from rich.console import Console
from proxybroker import Broker
import asyncio
import os

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
    while True:
        try:
            Proxy = await Proxies.get()
            if Proxy is None:
                break
            
            Logger.Info(f'Found Proxy: {list(Proxy.types)[0].lower()}://{Proxy.host}:{Proxy.port} - Country: {Proxy.location.country} - Anonymity: {Proxy.anonymity} - Source: {Proxy.source}')
            
            with open(f'{list(Proxy.types)[0].lower()}.txt', 'a') as File:
                os.makedirs('proxies', exist_ok=True)
                File.write(f'proxies/{list(Proxy.types)[0].lower()}://{Proxy.host}:{Proxy.port}\n')
            
        except Exception as e:
            Console().print_exception()
            Logger.Error(f'Error processing proxy: {e}')

Proxies = asyncio.Queue()
Broker = Broker(Proxies)
Tasks = asyncio.gather(
    Broker.find(types=['HTTP', 'HTTPS', 'SOCKS5'], limit=1000),
    Show(Proxies)
)

Loop = asyncio.get_event_loop()
Loop.run_until_complete(Tasks)