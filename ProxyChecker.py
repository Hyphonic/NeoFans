from rich.console import Console
from proxybroker import Broker
import aiofiles
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

async def Show(Proxies, ProxyType):
    try:
        os.makedirs('proxies', exist_ok=True)
        async with aiofiles.open(f'proxies/{ProxyType.lower()}.txt', 'w') as File:
            while True:
                try:
                    Proxy = await asyncio.wait_for(Proxies.get(), timeout=10.0)
                    if Proxy is None:
                        break
                    await File.write(f'{Proxy.host}:{Proxy.port}\n')
                    Logger.Info(f'Found {ProxyType} Proxy: {Proxy.host}:{Proxy.port}')
                except asyncio.TimeoutError:
                    Logger.Warning(f'Timeout waiting for {ProxyType} proxy')
                    break
                except Exception as e:
                    Logger.Error(f'Error processing {ProxyType} proxy: {str(e)}')
                    continue
                    
    except Exception as e:
        Logger.Error(f'Failed to save {ProxyType} proxies: {str(e)}')
                
    except Exception as e:
        Logger.Error(f'Failed to save {ProxyType} proxies: {str(e)}')

async def Main():
    for ProxyType in ['SOCKS5']:
        Proxies = asyncio.Queue()
        BrokerClient = Broker(Proxies)
        Tasks = asyncio.gather(
            BrokerClient.find(types=[ProxyType], limit=100),
            Show(Proxies, ProxyType)
        )
        await Tasks

if __name__ == '__main__':
    asyncio.run(Main())