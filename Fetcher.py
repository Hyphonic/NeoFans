# Main Imports
from dataclasses import dataclass
from typing import Union, Tuple
from json import JSONEncoder
from pathlib import Path
import aiofiles.os
import aiofiles
import asyncio
import aiohttp
import json
import os

# Logging
from rich.console import Console as RichConsole
from rich.traceback import install as Install
from rich.logging import RichHandler
from rich.theme import Theme
import logging

Console = RichConsole(theme=Theme({
    'log.time': 'bright_black',
    'logging.level.info': 'green',
    'logging.level.warning': 'yellow',
    'logging.level.error': 'red'
}),
    force_terminal=True,
    width=100,
    log_path=False
)

Handler = RichHandler(
    markup=True,
    rich_tracebacks=True,
    show_time=True,
    console=Console,
    show_path=False,
    omit_repeated_times=True
)

ConsoleHandler = RichHandler(
    markup=True,
    rich_tracebacks=True,
    show_time=True,
    console=Console,
    show_path=False,
    omit_repeated_times=True
)

ConsoleHandler.setFormatter(logging.Formatter(
    '%(message)s',
    datefmt='[%H:%M:%S]'
))

Handler.setFormatter(logging.Formatter(
    '%(asctime)s %(message)s',
    datefmt='[%H:%M:%S]'
))

logging.basicConfig(
    level=logging.INFO,
    handlers=[ConsoleHandler],
    force=True
)

# Configure Rich Logger
Log = logging.getLogger('rich')
Log.handlers.clear()
Log.addHandler(ConsoleHandler)
Log.propagate = False

def ErrorLogger(Error: Exception) -> None: 
    Console.print_exception(
        max_frames=1, 
        show_locals=True, 
        width=Console.width if Console.width else 100
    )

Install(show_locals=True)

@dataclass
class FileData:
    ID: Union[str, int]
    Name: str
    Url: str
    Path: Path
    Hash: Union[str, int, Tuple[str, int]]
    Extension: str

@dataclass
class CreatorData:
    ID: Union[str, int]
    Name: str
    Platform: str
    Service: str

# Fetcher Class
class Fetcher:
    def __init__(self, Session: aiohttp.ClientSession, Log: logging.Logger, ErrorLogger: logging.Logger) -> None:
        self.Log = Log
        self.ErrorLogger = ErrorLogger
        self.Session = Session
        self.Limit = 1
        self.TotalFiles = 0
        self.Data = {
            'coomer':
                {
                    'BaseUrl': 'https://coomer.su/api/v1',
                    'FileUrl': 'https://coomer.su',
                    'Session': os.getenv('COOMER_SESS'),
                    'Services': ['onlyfans', 'fansly'],
                    'Creators': {
                        'onlyfans': [],
                        'fansly': []
                    },
                    'Directory': {
                        'onlyfans': '🌀 OnlyFans',
                        'fansly': '🔒 Fansly'
                    },
                    'Posts':
                        {
                            'onlyfans': [],
                            'fansly': []
                        }
                },
            'kemono':
                {
                    'BaseUrl': 'https://kemono.su/api/v1',
                    'FileUrl': 'https://kemono.su',
                    'Session': os.getenv('KEMONO_SESS'),
                    'Services': ['patreon', 'subscribestar', 'gumroad', 'fanbox'],
                    'Creators': {
                        'patreon': [],
                        'subscribestar': [],
                        'gumroad': [],
                        'fanbox': []
                    },
                    'Directory': {
                        'patreon': '🅿️ Patreon',
                        'subscribestar': '⭐ SubscribeStar',
                        'gumroad': '🍬 Gumroad',
                        'fanbox': '📦 Fanbox'
                    },
                    'Posts':    {
                            'patreon': [],
                            'subscribestar': [],
                            'gumroad': [],
                            'fanbox': []
                    }
                }
            }

    async def Favorites(self) -> None:
        self.Log.info('Fetching Favorites...')
        Counter = 0
        Tasks = []
        
        async def Fetch(Platform: str, BaseUrl: str) -> None:
            nonlocal Counter
            try:
                async with self.Session.get(
                    f'{BaseUrl}/account/favorites?type=artist',
                    cookies={'session': self.Data[Platform]['Session']}
                ) as Response:
                    if Response.status == 200:
                        Creators = await Response.json()
                        for Creator in Creators:
                            self.Data[Platform]['Creators'][Creator['service']].append(
                                CreatorData(
                                    ID=Creator['id'],
                                    Name=Creator['name'].title(),
                                    Platform=Platform,
                                    Service=Creator['service']
                                )
                            )
                            Counter += 1
            except Exception as Error:
                self.ErrorLogger(Error)
                self.Log.warning(f'Failed To Fetch Favorites From {Platform.capitalize()}')

        for Platform in self.Data:
            Tasks.append(Fetch(Platform, self.Data[Platform]['BaseUrl']))
        
        await asyncio.gather(*Tasks)
        self.Log.info(f'Fetched {Counter} Favorites')
    
    async def Posts(self, Creator: CreatorData) -> None:
        self.Log.info(f'Fetching Posts From {Creator.Name}... ({Creator.ID})/{Creator.Service}')
        Counter = 0
        
        async def Fetch(Creator: CreatorData) -> None:
            nonlocal Counter
            try:
                async with self.Session.get(
                    f'{self.Data[Creator.Platform]["BaseUrl"]}/{Creator.Service}/user/{Creator.ID}/posts',
                    cookies={'session': self.Data[Creator.Platform]['Session']}
                ) as Response:
                    if Response.status == 200:
                        Posts = await Response.json()
                        for Post in Posts:
                            if Post.get('file') and Post['file'].get('path'):
                                FilePath = Path(Post['file']['path'])
                                FileInfo = FileData(
                                    ID=Creator.ID,
                                    Name=Creator.Name,
                                    Url=f'{self.Data[Creator.Platform]["FileUrl"]}{Post["file"]["path"]}',
                                    Path=Path(f'Data/{self.Data[Creator.Platform]["Directory"][Creator.Service]}/{Creator.Name}'),
                                    Hash=FilePath.stem,
                                    Extension=FilePath.suffix
                                )
                                self.Data[Creator.Platform]['Posts'][Creator.Service].append(FileInfo)
                                Counter += 1
                                self.TotalFiles += 1
                            
                            for Attachment in Post.get('attachments', []):
                                if Attachment.get('path'):
                                    FilePath = Path(Attachment['path'])
                                    FileInfo = FileData(
                                        ID=Creator.ID,
                                        Name=Creator.Name,
                                        Url=f'{self.Data[Creator.Platform]["FileUrl"]}{Attachment["path"]}',
                                        Path=Path(f'Data/{self.Data[Creator.Platform]["Directory"][Creator.Service]}/{Creator.Name}'),
                                        Hash=FilePath.stem,
                                        Extension=FilePath.suffix
                                    )
                                    self.Data[Creator.Platform]['Posts'][Creator.Service].append(FileInfo)
                                    Counter += 1
                                    self.TotalFiles += 1
            except Exception as Error:
                self.ErrorLogger(Error)
                self.Log.warning(f'Failed To Fetch Posts From {Creator.Name}')
        
        await Fetch(Creator)
        self.Log.info(f'Fetched {Counter} Posts From {Creator.Name}')

# Downloader Class
class Downloader:
    def __init__(self, Session: aiohttp.ClientSession, Log: logging.Logger, ErrorLogger: logging.Logger) -> None:
        self.Log = Log
        self.ErrorLogger = ErrorLogger
        self.Session = Session
        self.Semaphore = asyncio.Semaphore(5)
        try:
            with open('Data/Hashes.json', 'r') as Hashes:
                self.Hashes = set(json.load(Hashes))
        except FileNotFoundError:
            self.Hashes = set()
    
    async def Download(self, File: FileData) -> None:
        if str(File.Hash) in self.Hashes:
            return

        async with self.Semaphore:
            try:
                OutPath = Path('Data/Files') / File.Path.relative_to('Data')
                await aiofiles.os.makedirs(OutPath, exist_ok=True)
                
                async with self.Session.get(File.Url) as Response:
                    if Response.status == 200:
                        async with aiofiles.open(OutPath / f'{File.Hash}{File.Extension}', 'wb') as F:
                            await F.write(await Response.read())
                            self.Hashes.add(str(File.Hash))
                            self.Log.info(f'Downloaded [bold cyan]{File.Hash}[/]')
                            async with aiofiles.open('Data/Hashes.json', 'w') as f:
                                await f.write(json.dumps(list(self.Hashes)))
            except Exception as Error:
                self.ErrorLogger(Error)

# JSON Encoder
class Encoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Path):
            return str(obj)
        if hasattr(obj, '__dataclass_fields__'):
            return {k: str(getattr(obj, k)) if isinstance(getattr(obj, k), Path) 
                   else getattr(obj, k) for k in obj.__dataclass_fields__}
        return super().default(obj)

if __name__ == '__main__':
    async def Main() -> None:
        async with aiohttp.ClientSession() as Session:
            Fetch = Fetcher(Session, Log, ErrorLogger)
            Download = Downloader(Session, Log, ErrorLogger)
            await Fetch.Favorites()

            Fetch.Data = {Platform: {**Fetch.Data[Platform],
                                    'Creators': {Service: Creators[:Fetch.Limit] for Service,
                                    Creators in Fetch.Data[Platform]['Creators'].items()}}
                                    for Platform in Fetch.Data
                                    }

            for Platform in Fetch.Data:
                for Service in Fetch.Data[Platform]['Creators']:
                    for Creator in Fetch.Data[Platform]['Creators'][Service]:
                        await Fetch.Posts(Creator)
            
            Fetch.Log.info(f'Fetched [bold cyan]{Fetch.TotalFiles}[/] Files')

            await aiofiles.os.makedirs('Data/Files', exist_ok=True)
            Tasks = []
            for Platform in Fetch.Data:
                for Service in Fetch.Data[Platform]['Posts']:
                    for File in Fetch.Data[Platform]['Posts'][Service]:
                        Tasks.append(Download.Download(File))
            await asyncio.gather(*Tasks)

            await aiofiles.os.makedirs('Data', exist_ok=True)
            async with aiofiles.open('Data/Data.json', 'w', encoding='utf-8') as File:
                await File.write(json.dumps(
                    Fetch.Data,
                    indent=4,
                    ensure_ascii=False,
                    cls=Encoder
                    ))

    try:
        asyncio.run(Main())
    except KeyboardInterrupt:
        Log.info('Exiting...')