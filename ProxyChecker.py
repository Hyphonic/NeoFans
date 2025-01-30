from rich.console import Console
from proxybroker import Broker
import asyncio
import aiohttp
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

async def Verify(Proxy):
    try:
        async with aiohttp.ClientSession() as Session:
            async with Session.get('https://httpbin.org/ip', proxy=f'{list(Proxy.types)[0].lower()}://{Proxy.host}:{Proxy.port}', timeout=10) as Response:
                if Response.status == 200:
                    return True
                return False
    except Exception:
        Logger.Debug(f'∙ Failed to verify Proxy: {list(Proxy.types)[0].lower()}://{Proxy.host}:{Proxy.port}')
        Console().print_exception(max_frames=1)
        return False

async def Show(Proxies, Limit):
    Count = Limit
    while True:
        try:
            Proxy = await Proxies.get()
            if Proxy is None:
                break

            Count -= 1
            Logger.Debug(f'∙ [{Limit - Count}/{Limit}] Found Proxy: {list(Proxy.types)[0].lower()}://{Proxy.host}:{Proxy.port} - Status: ' + ('[green]Verified[/green]' if await Verify(Proxy) else '[red]Failed[/red]'))
            
            os.makedirs('proxies', exist_ok=True)
            with open(f'proxies/{list(Proxy.types)[0].lower()}.txt', 'a') as File:
                File.write(f'{list(Proxy.types)[0].lower()}://{Proxy.host}:{Proxy.port}\n')
            
        except Exception as e:
            Console().print_exception(max_frames=1)
            Logger.Error(f'Error processing proxy: {e}')

Limit = 100

Logger.Info(f'Scraping {Limit} Proxies:')

for ProxyType in ['http', 'https', 'socks5']:
    FilePath = f'proxies/{ProxyType}.txt'
    if os.path.exists(FilePath):
        with open(FilePath, 'w') as File:
            File.truncate()

Proxies = asyncio.Queue()
Broker = Broker(
    Proxies,
    providers=[
        'https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/all/data.txt', # All types
        'https://raw.githubusercontent.com/monosans/proxy-list/refs/heads/main/proxies_anonymous/all.txt', # All types (Anonymous)
        'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/http.txt', # HTTP
        'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/refs/heads/master/http.txt', # HTTP
        'https://raw.githubusercontent.com/mmpx12/proxy-list/refs/heads/master/proxies.txt', # All types
        'https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/http.txt', # HTTP
        'https://raw.githubusercontent.com/officialputuid/KangProxy/refs/heads/KangProxy/http/http.txt' # HTTP
        ],
    max_conn=400,
    timeout=10,
    max_tries=1,
    )
Tasks = asyncio.gather(
    Broker.find(types=[('HTTP', ('Anonymous', 'High')), 'HTTPS', 'SOCKS5'], limit=Limit),
    Show(Proxies, Limit)
)

Loop = asyncio.get_event_loop()
Loop.run_until_complete(Tasks)