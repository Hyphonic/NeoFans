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

async def Show(Proxies, Limit):
    Count = Limit
    while True:
        try:
            Proxy = await Proxies.get()
            if Proxy is None:
                break

            Count -= 1
            Logger.Debug(f'∙ [{Limit - Count}/{Limit}] Found Proxy: {list(Proxy.types)[0].lower()}://{Proxy.host}:{Proxy.port}')
            
            os.makedirs('proxies', exist_ok=True)
            with open(f'proxies/{list(Proxy.types)[0].lower()}.txt', 'a') as File:
                File.write(f'{list(Proxy.types)[0].lower()}://{Proxy.host}:{Proxy.port}\n')
            
        except Exception as e:
            Console().print_exception(max_frames=1)
            Logger.Error(f'Error processing proxy: {e}')

Limit = 1000

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
        'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/refs/heads/master/http.txt',
            'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/http.txt',
            'https://raw.githubusercontent.com/mmpx12/proxy-list/refs/heads/master/http.txt',
            'https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/http/data.txt',
            'https://raw.githubusercontent.com/officialputuid/KangProxy/refs/heads/KangProxy/http/http.txt',
            'https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/http.txt',
            'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/http.txt',
            'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=1000',
            'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/refs/heads/master/socks4.txt',
            'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/socks4.txt',
            'https://raw.githubusercontent.com/mmpx12/proxy-list/refs/heads/master/socks4.txt',
            'https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/socks4/data.txt',
            'https://raw.githubusercontent.com/officialputuid/KangProxy/refs/heads/KangProxy/socks4/socks4.txt',
            'https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/socks4.txt',
            'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/socks4.txt',
            'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=1000',
            'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/refs/heads/master/socks5.txt',
            'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/socks5.txt',
            'https://raw.githubusercontent.com/mmpx12/proxy-list/refs/heads/master/socks5.txt',
            'https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/socks5/data.txt',
            'https://raw.githubusercontent.com/officialputuid/KangProxy/refs/heads/KangProxy/socks5/socks5.txt',
            'https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/socks5.txt',
            'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/socks5.txt',
            'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=1000'
        ],
    max_conn=500,
    timeout=10,
    max_tries=1,
    )
Tasks = asyncio.gather(
    Broker.find(types=[('HTTP', ('Anonymous', 'High')), 'HTTPS', 'SOCKS5'], limit=Limit),
    Show(Proxies, Limit)
)

Loop = asyncio.get_event_loop()
Loop.run_until_complete(Tasks)