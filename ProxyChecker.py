from rich.console import Console
from proxybroker import Broker
import asyncio
import aiohttp
import json
import os
import re

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

Limit = 500

Logger.Info(f'Scraping {Limit} Proxies:')

for ProxyType in ['http', 'https', 'socks5']:
    FilePath = f'proxies/{ProxyType}.txt'
    if os.path.exists(FilePath):
        with open(FilePath, 'w') as File:
            File.truncate()

async def GetProxies(Provider):
    ProxyLists = {
        'http': [],
        'socks4': [],
        'socks5': []
    }

    for Type in Provider:
        if Type == 'direct':
            for Protocol in Provider[Type]:
                for Url in Provider[Type][Protocol]:
                    Logger.Debug(f'Getting Proxies from: {Url}')
                    async with aiohttp.ClientSession() as Session:
                        async with Session.get(Url) as Response:
                            if Response.status == 200:
                                Proxies = await Response.text()
                                for Proxy in Proxies.split('\n'):
                                    Proxy = Proxy.strip()
                                    if Proxy:
                                        # Strip any existing protocol prefix
                                        CleanProxy = re.sub(r'^(http|socks4|socks5)://', '', Proxy)
                                        ProxyLists[Protocol].append(CleanProxy)
                            else:
                                Logger.Error(f'Failed to get proxies from: {Url}')

        elif Type == 'indirect':
            for Protocol in Provider[Type]:
                for Url in Provider[Type][Protocol]:
                    Logger.Debug(f'Fetching Indirect Proxies from: {Url}')
                    async with aiohttp.ClientSession() as Session:
                        async with Session.get(Url) as Response:
                            if Response.status == 200:
                                try:
                                    Data = await Response.json()
                                    for Entry in Data.get("data", []):
                                        if "ipPort" in Entry:
                                            Proxy = Entry["ipPort"]
                                        elif "ip" in Entry and "port" in Entry:
                                            Proxy = f'{Entry["ip"]}:{Entry["port"]}'
                                        else:
                                            continue
                                        
                                        # Strip any existing protocol prefix
                                        CleanProxy = re.sub(r'^(http|socks4|socks5)://', '', Proxy)
                                        ProxyLists[Protocol].append(CleanProxy)
                                except json.JSONDecodeError:
                                    Logger.Error(f'Invalid JSON from: {Url}')
                            else:
                                Logger.Error(f'Failed to get proxies from: {Url}')

    # Format proxies with correct protocol prefix
    FormattedProxies = []
    for Protocol, ProxyList in ProxyLists.items():
        FormattedProxies.extend([f'{Protocol}://{proxy}' for proxy in ProxyList])

    return FormattedProxies

Providers = [
    {
        'direct': {
            'http': [
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/refs/heads/master/http.txt',
                'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/http.txt',
                'https://raw.githubusercontent.com/mmpx12/proxy-list/refs/heads/master/http.txt',
                'https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/http/data.txt',
                'https://raw.githubusercontent.com/officialputuid/KangProxy/refs/heads/KangProxy/http/http.txt',
                'https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/http.txt',
                'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/http.txt',
                'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=1000'
            ],
            'socks4': [
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/refs/heads/master/socks4.txt',
                'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/socks4.txt',
                'https://raw.githubusercontent.com/mmpx12/proxy-list/refs/heads/master/socks4.txt',
                'https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/socks4/data.txt',
                'https://raw.githubusercontent.com/officialputuid/KangProxy/refs/heads/KangProxy/socks4/socks4.txt',
                'https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/socks4.txt',
                'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/socks4.txt',
                'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=1000'
            ],
            'socks5': [
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/refs/heads/master/socks5.txt',
                'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/socks5.txt',
                'https://raw.githubusercontent.com/mmpx12/proxy-list/refs/heads/master/socks5.txt',
                'https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/protocols/socks5/data.txt',
                'https://raw.githubusercontent.com/officialputuid/KangProxy/refs/heads/KangProxy/socks5/socks5.txt',
                'https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/socks5.txt',
                'https://raw.githubusercontent.com/ErcinDedeoglu/proxies/refs/heads/main/proxies/socks5.txt',
                'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=1000'
            ]
        },
        'indirect': {
            'http': [
                'http://pubproxy.com/api/proxy?type=http', # {"data":[{"ipPort":"<ip>:<port>"}]}

            ],
            'socks4': [
                'http://pubproxy.com/api/proxy?type=socks4', # {"data":[{"ipPort":"<ip>:<port>"}]}
            ],
            'socks5': [
                'http://pubproxy.com/api/proxy?type=socks5', # {"data":[{"ipPort":"<ip>:<port>"}]}
            ]
        }
    }
]

async def Main():
    # Get proxies first
    AllProxies = []
    for Provider in Providers:
        Proxies = await GetProxies(Provider)
        AllProxies.extend(list(set(Proxies)))
    
    Logger.Info(f'Found {len(AllProxies)} Proxies.')
    
    # Setup queue and broker
    Queue = asyncio.Queue()
    ProxyBroker = Broker(
        Queue,
        max_conn=500,
        timeout=10,
        max_tries=1
    )
    
    # Run tasks with required types parameter
    await asyncio.gather(
        ProxyBroker.find(
            types=['HTTP', 'HTTPS', 'SOCKS5'],  # Required parameter
            data=AllProxies,  # Optional data parameter
            limit=Limit
        ),
        Show(Queue, Limit)
    )

if __name__ == '__main__':
    asyncio.run(Main())